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
    MESSAGE_CONSUMER_MOLECULE_ANNOUNCEMENT_SEARCH_UPDATER,
    MESSAGE_PRODUCER_MOLECULE_ANNOUNCEMENT_SERVICE,
    MoleculeAnnouncementMessage,
    MoleculeAnnouncementMessagePublished,
    molecule_announcement_search_schema as announcement_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::*;

use crate::search::indexers::index_announcement_from_publication_record;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeAnnouncementMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_MOLECULE_ANNOUNCEMENT_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_MOLECULE_ANNOUNCEMENT_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct MoleculeAnnouncementSearchUpdater {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeAnnouncementSearchUpdater {
    async fn handle_published_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        published_message: &MoleculeAnnouncementMessagePublished,
    ) -> Result<(), InternalError> {
        let announcement_document = index_announcement_from_publication_record(
            published_message.event_time,
            published_message.system_time,
            &published_message.molecule_account_id,
            &published_message.ipnft_uid,
            &published_message.announcement_record,
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

impl MessageConsumer for MoleculeAnnouncementSearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeAnnouncementMessage> for MoleculeAnnouncementSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeAnnouncementSearchUpdater[MoleculeAnnouncementMessage]"
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
