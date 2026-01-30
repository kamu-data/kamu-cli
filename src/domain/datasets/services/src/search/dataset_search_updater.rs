// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    AccountLifecycleMessage,
    AccountLifecycleMessageUpdated,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
};
use kamu_auth_rebac::{
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
    RebacDatasetPropertiesMessage,
    RebacDatasetRelationsMessage,
    RebacService,
};
use kamu_datasets::*;
use kamu_search::*;
use messaging_outbox::*;

use crate::search::dataset_search_indexer::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn messaging_outbox::MessageConsumer)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<DatasetLifecycleMessage>)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<DatasetReferenceMessage>)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<AccountLifecycleMessage>)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<RebacDatasetPropertiesMessage>)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<RebacDatasetRelationsMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASETS_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
        MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetSearchUpdater {
    search_service: Arc<dyn SearchService>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    rebac_service: Arc<dyn RebacService>,
    embeddings_provider: Arc<dyn EmbeddingsProvider>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetSearchUpdater {
    async fn handle_dataset_ref_updated(
        &self,
        catalog: &dill::Catalog,
        updated_message: &DatasetReferenceMessageUpdated,
    ) -> Result<(), InternalError> {
        // Prepare indexing helper
        let indexing_helper = DatasetIndexingHelper {
            embeddings_provider: self.embeddings_provider.as_ref(),
            rebac_service: self.rebac_service.as_ref(),
        };

        // Resolve entry
        let entry = self
            .dataset_entry_service
            .get_entry(&updated_message.dataset_id)
            .await
            .int_err()?;

        // Resolve dataset
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(&updated_message.dataset_id)
            .await
            .int_err()?;

        // Did we have updates before?
        if updated_message.maybe_prev_block_hash.is_some() {
            // Find existing document and prepare partial update
            let maybe_existing_document = self
                .search_service
                .find_document_by_id(
                    SearchContext::unrestricted(catalog),
                    dataset_search_schema::SCHEMA_NAME,
                    &updated_message.dataset_id.to_string(),
                )
                .await
                .int_err()?;

            // Is document present?
            // Note, even if we had previous updates, the document may be missing,
            // if it's empty (does not contain any useful indexable fields).
            // In this case, we need to reindex from scratch.
            if let Some(existing_document) = maybe_existing_document {
                // Prepare partial update to dataset search document
                let partial_update = indexing_helper
                    .partial_update_for_new_interval(
                        dataset,
                        &entry.owner_id,
                        &updated_message.new_block_hash,
                        updated_message.maybe_prev_block_hash.as_ref(),
                        existing_document,
                    )
                    .await?;

                // Send to search engine
                self.search_service
                    .bulk_update(
                        SearchContext::unrestricted(catalog),
                        dataset_search_schema::SCHEMA_NAME,
                        vec![SearchIndexUpdateOperation::Update {
                            id: updated_message.dataset_id.to_string(),
                            doc: partial_update,
                        }],
                    )
                    .await
                    .int_err()?;

                return Ok(());
            }

            tracing::info!(
                dataset_id = %updated_message.dataset_id,
                "Dataset search document not found during partial update, falling back to full reindex",
            );
        }

        // Reindex from scratch
        let dataset_document = indexing_helper
            .index_dataset_from_scratch(dataset, &entry.owner_id, &updated_message.new_block_hash)
            .await?;

        // Send to search engine
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Index {
                    id: updated_message.dataset_id.to_string(),
                    doc: dataset_document,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_renamed_dataset(
        &self,
        catalog: &dill::Catalog,
        renamed_message: &DatasetLifecycleMessageRenamed,
    ) -> Result<(), InternalError> {
        // Prepare partial update to dataset search document
        let partial_update = partial_update_for_rename(&renamed_message.new_dataset_alias);

        // Send to search engine
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: renamed_message.dataset_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
            .int_err()
    }

    async fn handle_deleted_dataset(
        &self,
        catalog: &dill::Catalog,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Delete {
                    id: dataset_id.to_string(),
                }],
            )
            .await
            .int_err()
    }

    async fn handle_renamed_account(
        &self,
        catalog: &dill::Catalog,
        updated_account_message: &AccountLifecycleMessageUpdated,
    ) -> Result<(), InternalError> {
        // Find all datasets owned by this account
        use futures::TryStreamExt;
        let entries: Vec<DatasetEntry> = self
            .dataset_entry_service
            .entries_owned_by(&updated_account_message.account_id)
            .try_collect()
            .await
            .int_err()?;

        // Prepare partial updates to dataset search documents
        let mut updates = Vec::new();
        for entry in entries {
            let new_alias = odf::DatasetAlias::new(
                Some(updated_account_message.new_account_name.clone()),
                entry.name.clone(),
            );
            let partial_update = partial_update_for_rename(&new_alias);
            updates.push(SearchIndexUpdateOperation::Update {
                id: entry.id.to_string(),
                doc: partial_update,
            });
        }

        // Send to search engine
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                updates,
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_rebac_dataset_properties_modified(
        &self,
        catalog: &dill::Catalog,
        dataset_id: &odf::DatasetID,
        properties: &kamu_auth_rebac::DatasetProperties,
    ) -> Result<(), InternalError> {
        // Find existing document
        let maybe_existing_document = self
            .search_service
            .find_document_by_id(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                &dataset_id.to_string(),
            )
            .await
            .int_err()?;

        // Skip if document is not present.
        // ReBAC properties would be taken into account during first set_ref()
        if maybe_existing_document.is_none() {
            tracing::info!(
                dataset_id = %dataset_id,
                "Dataset search document not found during ReBAC properties update, skipping",
            );
            return Ok(());
        }

        // Prepare indexing helper
        let indexing_helper = DatasetIndexingHelper {
            embeddings_provider: self.embeddings_provider.as_ref(),
            rebac_service: self.rebac_service.as_ref(),
        };

        // Resolve entry
        let entry = self
            .dataset_entry_service
            .get_entry(dataset_id)
            .await
            .int_err()?;

        // Resolve dataset
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(dataset_id)
            .await
            .int_err()?;

        // Prepare partial update to dataset search document
        let partial_update = indexing_helper
            .partial_update_for_rebac_properties_change(dataset, &entry.owner_id, properties)
            .await?;

        // Send to search engine
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: dataset_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_rebac_dataset_account_relations_properties_modified(
        &self,
        catalog: &dill::Catalog,
        dataset_id: &odf::DatasetID,
        authorized_accounts: &[kamu_auth_rebac::AuthorizedAccount],
    ) -> Result<(), InternalError> {
        // Find existing document
        let maybe_existing_document = self
            .search_service
            .find_document_by_id(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                &dataset_id.to_string(),
            )
            .await
            .int_err()?;

        // Skip if document is not present.
        // ReBAC properties would be taken into account during first set_ref()
        if maybe_existing_document.is_none() {
            tracing::info!(
                dataset_id = %dataset_id,
                "Dataset search document not found during ReBAC properties update, skipping",
            );
            return Ok(());
        }

        // Prepare indexing helper
        let indexing_helper = DatasetIndexingHelper {
            embeddings_provider: self.embeddings_provider.as_ref(),
            rebac_service: self.rebac_service.as_ref(),
        };

        // Resolve entry
        let entry = self
            .dataset_entry_service
            .get_entry(dataset_id)
            .await
            .int_err()?;

        // Resolve dataset
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(dataset_id)
            .await
            .int_err()?;

        // Prepare partial update to dataset search document
        let partial_update = indexing_helper
            .partial_update_for_rebac_account_relations_change(
                dataset,
                &entry.owner_id,
                authorized_accounts,
            )
            .await?;

        // Send to search engine
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: dataset_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetSearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetSearchUpdater[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Created(_) => {
                // Skip, we will use "set_ref" messages for indexing
            }
            DatasetLifecycleMessage::Renamed(renamed_message) => {
                self.handle_renamed_dataset(target_catalog, renamed_message)
                    .await?;
            }
            DatasetLifecycleMessage::Deleted(deleted_message) => {
                self.handle_deleted_dataset(target_catalog, &deleted_message.dataset_id)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetSearchUpdater[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        match message {
            DatasetReferenceMessage::Updated(updated_message) => {
                // We only care about head updates for search engine.
                if updated_message.block_ref != odf::BlockRef::Head {
                    return Ok(());
                }

                self.handle_dataset_ref_updated(target_catalog, updated_message)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DatasetSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetSearchUpdater[AccountLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received account lifecycle message");

        match message {
            AccountLifecycleMessage::Updated(updated_account_message) => {
                // Only account names are interesting for datasets search
                if updated_account_message.old_account_name
                    != updated_account_message.new_account_name
                {
                    self.handle_renamed_account(target_catalog, updated_account_message)
                        .await?;
                }
            }

            AccountLifecycleMessage::Created(_)
            | AccountLifecycleMessage::Deleted(_)
            | AccountLifecycleMessage::PasswordChanged(_) => {
                // No-op for search
                // Note: deletion of accounts will trigger deletion of datasets,
                // which we handle via DatasetLifecycleMessage
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<RebacDatasetPropertiesMessage> for DatasetSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetSearchUpdater[RebacDatasetPropertiesMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &RebacDatasetPropertiesMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received ReBAC dataset properties message");

        match message {
            RebacDatasetPropertiesMessage::Modified(message) => {
                self.handle_rebac_dataset_properties_modified(
                    target_catalog,
                    &message.dataset_id,
                    &message.dataset_properties,
                )
                .await?;
            }

            RebacDatasetPropertiesMessage::Deleted(_) => {
                // No-op for search
                // Note: deletion of datasets will trigger Rebac deletion,
                // which we handle via DatasetLifecycleMessage
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<RebacDatasetRelationsMessage> for DatasetSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetSearchUpdater[RebacDatasetAccountRelationsMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &RebacDatasetRelationsMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received ReBAC dataset account relations message");

        match message {
            RebacDatasetRelationsMessage::Modified(message) => {
                self.handle_rebac_dataset_account_relations_properties_modified(
                    target_catalog,
                    &message.dataset_id,
                    &message.authorized_accounts,
                )
                .await?;
            }

            RebacDatasetRelationsMessage::Deleted(_) => {
                // No-op for search
                // Note: deletion of datasets will trigger Rebac deletion,
                // which we handle via DatasetLifecycleMessage
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
