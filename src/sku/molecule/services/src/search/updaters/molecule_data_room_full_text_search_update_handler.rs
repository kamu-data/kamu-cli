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
    MoleculeDataRoomMessage,
    MoleculeDataRoomMessageEntryCreated,
    MoleculeDataRoomMessageEntryMoved,
    MoleculeDataRoomMessageEntryRemoved,
    MoleculeDataRoomMessageEntryUpdated,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::{MessageConsumer, MessageConsumerT};

use crate::search::molecule_full_text_search_schema_helpers as schema_helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeDataRoomMessage>)]
pub struct MoleculeDataRoomFullTextSearchUpdateHandler {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDataRoomFullTextSearchUpdateHandler {
    async fn handle_created_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        created_message: &MoleculeDataRoomMessageEntryCreated,
    ) -> Result<(), InternalError> {
        let data_room_entry_document = schema_helpers::index_data_room_entry_from_entity(
            &created_message.ipnft_uid,
            &created_message.data_room_entry,
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Index {
                    id: data_room_entry_schema::unique_id(
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
        let data_room_entry_document = schema_helpers::index_data_room_entry_from_entity(
            &updated_message.ipnft_uid,
            &updated_message.data_room_entry,
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Update {
                    id: data_room_entry_schema::unique_id(
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
        self.full_text_search_service
            .bulk_update(
                ctx,
                data_room_entry_schema::SCHEMA_NAME,
                vec![
                    FullTextUpdateOperation::Delete {
                        id: data_room_entry_schema::unique_id(
                            &moved_message.ipnft_uid,
                            &moved_message.path_from,
                        ),
                    },
                    FullTextUpdateOperation::Index {
                        id: data_room_entry_schema::unique_id(
                            &moved_message.ipnft_uid,
                            &moved_message.path_to,
                        ),
                        doc: schema_helpers::index_data_room_entry_from_entity(
                            &moved_message.ipnft_uid,
                            &moved_message.data_room_entry,
                        ),
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
                    id: data_room_entry_schema::unique_id(
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

impl MessageConsumer for MoleculeDataRoomFullTextSearchUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeDataRoomMessage> for MoleculeDataRoomFullTextSearchUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeDataRoomFullTextSearchUpdateHandler[MoleculeDataRoomMessage]"
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
