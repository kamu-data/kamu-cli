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
use kamu_molecule_domain::{
    MESSAGE_CONSUMER_MOLECULE_DATA_ROOM_SEARCH_UPDATER,
    MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
    MoleculeDataRoomMessage,
    MoleculeDataRoomMessageEntryCreated,
    MoleculeDataRoomMessageEntryMoved,
    MoleculeDataRoomMessageEntryRemoved,
    MoleculeDataRoomMessageEntryUpdated,
    molecule_data_room_entry_search_schema as data_room_entry_schema,
    molecule_search_schema_common as molecule_schema,
};
use kamu_search::{EmbeddingsProvider, SearchContext, SearchIndexUpdateOperation, SearchService};
use messaging_outbox::*;

use crate::search::indexers::MoleculeDataRoomEntryIndexingHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeDataRoomMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_MOLECULE_DATA_ROOM_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct MoleculeDataRoomSearchUpdater {
    search_service: Arc<dyn SearchService>,
    embeddings_provider: Arc<dyn EmbeddingsProvider>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDataRoomSearchUpdater {
    async fn handle_created_message(
        &self,
        ctx: SearchContext<'_>,
        created_message: &MoleculeDataRoomMessageEntryCreated,
    ) -> Result<(), InternalError> {
        let indexing_helper = MoleculeDataRoomEntryIndexingHelper {
            molecule_account_id: &created_message.molecule_account_id,
            embeddings_provider: self.embeddings_provider.as_ref(),
        };

        let data_room_entry_document = indexing_helper
            .index_data_room_entry_from_entity(
                &created_message.ipnft_uid,
                &created_message.data_room_entry,
                created_message.content_text.as_ref(),
            )
            .await?;

        self.search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Index {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &created_message.ipnft_uid,
                        &created_message.data_room_entry.path,
                    ),
                    doc: data_room_entry_document,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_updated_message(
        &self,
        ctx: SearchContext<'_>,
        updated_message: &MoleculeDataRoomMessageEntryUpdated,
    ) -> Result<(), InternalError> {
        let indexing_helper = MoleculeDataRoomEntryIndexingHelper {
            molecule_account_id: &updated_message.molecule_account_id,
            embeddings_provider: self.embeddings_provider.as_ref(),
        };

        let data_room_entry_document = indexing_helper
            .index_data_room_entry_from_entity(
                &updated_message.ipnft_uid,
                &updated_message.data_room_entry,
                updated_message.content_text.as_ref(),
            )
            .await?;

        self.search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &updated_message.ipnft_uid,
                        &updated_message.data_room_entry.path,
                    ),
                    doc: data_room_entry_document,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_moved_message(
        &self,
        ctx: SearchContext<'_>,
        moved_message: &MoleculeDataRoomMessageEntryMoved,
    ) -> Result<(), InternalError> {
        let old_id = data_room_entry_schema::unique_id_for_data_room_entry(
            &moved_message.ipnft_uid,
            &moved_message.path_from,
        );

        let maybe_existing_document = self
            .search_service
            .find_document_by_id(ctx.clone(), data_room_entry_schema::SCHEMA_NAME, &old_id)
            .await
            .int_err()?;

        let Some(existing_document) = maybe_existing_document else {
            tracing::warn!(
                ipnft_uid = moved_message.ipnft_uid.as_str(),
                path_from = moved_message.path_from.as_str(),
                "Could not find document for moved data room entry at its old location. Skipping \
                 move handling.",
            );
            return Ok(());
        };

        let new_id = data_room_entry_schema::unique_id_for_data_room_entry(
            &moved_message.ipnft_uid,
            &moved_message.path_to,
        );

        // Replace modified properties
        let mut existing_document = existing_document;
        if let Some(obj) = existing_document.as_object_mut() {
            obj.insert(
                molecule_schema::fields::PATH.to_string(),
                serde_json::Value::String(moved_message.path_to.as_str().to_string()),
            );
            obj.insert(
                molecule_schema::fields::SYSTEM_TIME.to_string(),
                serde_json::Value::String(
                    moved_message
                        .data_room_entry
                        .system_time
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                ),
            );
            obj.insert(
                molecule_schema::fields::EVENT_TIME.to_string(),
                serde_json::Value::String(
                    moved_message
                        .data_room_entry
                        .event_time
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                ),
            );
            obj.insert(
                molecule_schema::fields::CHANGE_BY.to_string(),
                serde_json::Value::String(
                    moved_message
                        .data_room_entry
                        .denormalized_latest_file_info
                        .change_by
                        .clone(),
                ),
            );
        } else {
            unreachable!()
        }

        self.search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![
                    SearchIndexUpdateOperation::Delete { id: old_id },
                    SearchIndexUpdateOperation::Index {
                        id: new_id,
                        doc: existing_document,
                    },
                ],
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_removed_message(
        &self,
        ctx: SearchContext<'_>,
        removed_message: &MoleculeDataRoomMessageEntryRemoved,
    ) -> Result<(), InternalError> {
        self.search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Delete {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &removed_message.ipnft_uid,
                        &removed_message.path,
                    ),
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MoleculeDataRoomSearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeDataRoomMessage> for MoleculeDataRoomSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeDataRoomSearchUpdater[MoleculeDataRoomMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &MoleculeDataRoomMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule data room message");

        let ctx = SearchContext::unrestricted(target_catalog);

        match message {
            MoleculeDataRoomMessage::EntryCreated(created_message) => {
                self.handle_created_message(ctx, created_message).await?;
            }
            MoleculeDataRoomMessage::EntryUpdated(updated_message) => {
                self.handle_updated_message(ctx, updated_message).await?;
            }
            MoleculeDataRoomMessage::EntryMoved(moved_message) => {
                self.handle_moved_message(ctx, moved_message).await?;
            }
            MoleculeDataRoomMessage::EntryRemoved(removed_message) => {
                self.handle_removed_message(ctx, removed_message).await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
