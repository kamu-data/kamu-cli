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
    MoleculeAnnouncementMessage,
    MoleculeAnnouncementMessagePublished,
    molecule_announcement_full_text_search_schema as announcement_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::{MessageConsumer, MessageConsumerT};

use crate::search::molecule_full_text_search_schema_helpers as schema_helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeAnnouncementMessage>)]
pub struct MoleculeAnnouncementFullTextSearchUpdateHandler {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeAnnouncementFullTextSearchUpdateHandler {
    async fn handle_published_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        published_message: &MoleculeAnnouncementMessagePublished,
    ) -> Result<(), InternalError> {
        let announcement_document = schema_helpers::index_announcement_from_publication_record(
            &published_message.ipnft_uid,
            &published_message.announcement_record,
            published_message.event_time, // consider propagating ingest time instead
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                announcement_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Index {
                    id: published_message.ipnft_uid.clone(),
                    doc: announcement_document,
                }],
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MoleculeAnnouncementFullTextSearchUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeAnnouncementMessage>
    for MoleculeAnnouncementFullTextSearchUpdateHandler
{
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeAnnouncementFullTextSearchUpdateHandler[MoleculeAnnouncementMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &MoleculeAnnouncementMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule announcement message");

        let ctx = FullTextSearchContext {
            catalog: target_catalog,
        };

        match message {
            MoleculeAnnouncementMessage::Published(published_message) => {
                self.handle_published_message(ctx, published_message)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
