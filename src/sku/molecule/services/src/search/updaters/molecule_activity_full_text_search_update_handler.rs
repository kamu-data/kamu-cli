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
    MoleculeActivityMessage,
    MoleculeActivityMessagePublished,
    molecule_activity_full_text_search_schema as activity_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::{MessageConsumer, MessageConsumerT};

use crate::search::molecule_full_text_search_schema_helpers as schema_helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeActivityMessage>)]
pub struct MoleculeActivityFullTextSearchUpdateHandler {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeActivityFullTextSearchUpdateHandler {
    async fn handle_published_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        published_message: &MoleculeActivityMessagePublished,
    ) -> Result<(), InternalError> {
        let activity_document = schema_helpers::index_activity_from_data_room_publication_record(
            &published_message.activity_record,
            published_message.event_time,
        );

        let id = activity_schema::unique_id_for_data_room_activity(
            &published_message.molecule_account_id,
            published_message.offset,
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                activity_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Index {
                    id,
                    doc: activity_document,
                }],
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MoleculeActivityFullTextSearchUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeActivityMessage> for MoleculeActivityFullTextSearchUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeActivityFullTextSearchUpdateHandler[MoleculeActivityMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &MoleculeActivityMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule activity message");

        let ctx = FullTextSearchContext {
            catalog: target_catalog,
        };

        match message {
            MoleculeActivityMessage::Published(published_message) => {
                self.handle_published_message(ctx, published_message)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
