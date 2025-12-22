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
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASETS_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetSearchUpdater {
    search_service: Arc<dyn SearchService>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetSearchUpdater {
    async fn handle_new_dataset(
        &self,
        ctx: SearchContext<'_>,
        created_message: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        // Resolve dataset
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(&created_message.dataset_id)
            .await
            .int_err()?;

        // Prepare dataset search document
        let dataset_document =
            index_dataset_from_scratch(dataset, &created_message.owner_account_id).await?;

        // Sent it to the full text search service for indexing
        self.search_service
            .bulk_update(
                ctx,
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Index {
                    id: created_message.dataset_id.to_string(),
                    doc: dataset_document,
                }],
            )
            .await
    }

    async fn handle_dataset_ref_updated(
        &self,
        ctx: SearchContext<'_>,
        updated_message: &DatasetReferenceMessageUpdated,
    ) -> Result<(), InternalError> {
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

        // Prepare partial update to dataset search document
        let partial_update = partial_update_for_new_interval(
            dataset,
            &entry.owner_id,
            &updated_message.new_block_hash,
            updated_message.maybe_prev_block_hash.as_ref(),
        )
        .await?;

        // Send it to the full text search service for updating
        self.search_service
            .bulk_update(
                ctx,
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: updated_message.dataset_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
    }

    async fn handle_renamed_dataset(
        &self,
        ctx: SearchContext<'_>,
        renamed_message: &DatasetLifecycleMessageRenamed,
    ) -> Result<(), InternalError> {
        // Prepare partial update to dataset search document
        let partial_update = partial_update_for_rename(&renamed_message.new_dataset_alias);

        // Send it to the full text search service for updating
        self.search_service
            .bulk_update(
                ctx,
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: renamed_message.dataset_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
    }

    async fn handle_deleted_dataset(
        &self,
        ctx: SearchContext<'_>,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.search_service
            .bulk_update(
                ctx,
                dataset_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Delete {
                    id: dataset_id.to_string(),
                }],
            )
            .await
    }

    async fn handle_renamed_account(
        &self,
        ctx: SearchContext<'_>,
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

        // Send them to the full text search service for updating
        self.search_service
            .bulk_update(ctx, dataset_search_schema::SCHEMA_NAME, updates)
            .await?;

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

        let ctx = SearchContext {
            catalog: target_catalog,
        };

        match message {
            DatasetLifecycleMessage::Created(created_message) => {
                self.handle_new_dataset(ctx, created_message).await?;
            }
            DatasetLifecycleMessage::Renamed(renamed_message) => {
                self.handle_renamed_dataset(ctx, renamed_message).await?;
            }
            DatasetLifecycleMessage::Deleted(deleted_message) => {
                self.handle_deleted_dataset(ctx, &deleted_message.dataset_id)
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

        let ctx = SearchContext {
            catalog: target_catalog,
        };

        match message {
            DatasetReferenceMessage::Updated(updated_message) => {
                // We only care about head updates for full text search.
                // Also, skip initial setting of head, that is handled with dataset message.
                if updated_message.block_ref != odf::BlockRef::Head
                    || updated_message.maybe_prev_block_hash.is_none()
                {
                    return Ok(());
                }

                self.handle_dataset_ref_updated(ctx, updated_message)
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

        let ctx = SearchContext {
            catalog: target_catalog,
        };

        match message {
            AccountLifecycleMessage::Updated(updated_account_message) => {
                // Only account names are interesting for datasets search
                if updated_account_message.old_account_name
                    != updated_account_message.new_account_name
                {
                    self.handle_renamed_account(ctx, updated_account_message)
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
