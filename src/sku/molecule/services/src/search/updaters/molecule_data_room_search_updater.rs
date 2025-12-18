// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_molecule_domain::{
    MESSAGE_CONSUMER_MOLECULE_DATA_ROOM_SEARCH_UPDATER,
    MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
    MoleculeDataRoomMessage,
    MoleculeDataRoomMessageEntryCreated,
    MoleculeDataRoomMessageEntryMoved,
    MoleculeDataRoomMessageEntryRemoved,
    MoleculeDataRoomMessageEntryUpdated,
    molecule_data_room_entry_search_schema as data_room_entry_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::*;

use crate::search::indexers::index_data_room_entry_from_entity;

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
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDataRoomSearchUpdater {
    async fn handle_created_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        created_message: &MoleculeDataRoomMessageEntryCreated,
    ) -> Result<(), InternalError> {
        let data_room_entry_document = index_data_room_entry_from_entity(
            &created_message.molecule_account_id,
            &created_message.ipnft_uid,
            &created_message.data_room_entry,
            created_message.content_text.as_ref(),
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Index {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &created_message.ipnft_uid,
                        &created_message.data_room_entry.path,
                    ),
                    doc: data_room_entry_document,
                }],
            )
            .await?;

        Ok(())
    }

    async fn handle_updated_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        updated_message: &MoleculeDataRoomMessageEntryUpdated,
    ) -> Result<(), InternalError> {
        let data_room_entry_document = index_data_room_entry_from_entity(
            &updated_message.molecule_account_id,
            &updated_message.ipnft_uid,
            &updated_message.data_room_entry,
            updated_message.content_text.as_ref(),
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Update {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &updated_message.ipnft_uid,
                        &updated_message.data_room_entry.path,
                    ),
                    doc: data_room_entry_document,
                }],
            )
            .await?;

        Ok(())
    }

    async fn handle_moved_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        moved_message: &MoleculeDataRoomMessageEntryMoved,
    ) -> Result<(), InternalError> {
        let old_id = data_room_entry_schema::unique_id_for_data_room_entry(
            &moved_message.ipnft_uid,
            &moved_message.path_from,
        );

        let maybe_existing_document = self
            .full_text_search_service
            .find_document_by_id(ctx, data_room_entry_schema::SCHEMA_NAME, &old_id)
            .await?;

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

        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![
                    FullTextUpdateOperation::Delete { id: old_id },
                    FullTextUpdateOperation::Index {
                        id: new_id,
                        doc: existing_document,
                    },
                ],
            )
            .await?;

        Ok(())
    }

    async fn handle_removed_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        removed_message: &MoleculeDataRoomMessageEntryRemoved,
    ) -> Result<(), InternalError> {
        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Delete {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &removed_message.ipnft_uid,
                        &removed_message.path,
                    ),
                }],
            )
            .await?;

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

        let ctx = FullTextSearchContext {
            catalog: target_catalog,
        };

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
