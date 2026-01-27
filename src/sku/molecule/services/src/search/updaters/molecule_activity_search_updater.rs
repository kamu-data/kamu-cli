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
    MESSAGE_CONSUMER_MOLECULE_ACTIVITY_SEARCH_UPDATER,
    MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
    MoleculeActivityMessage,
    MoleculeActivityMessagePublished,
    molecule_activity_search_schema as activity_schema,
};
use kamu_search::{SearchContext, SearchIndexUpdateOperation, SearchService};
use messaging_outbox::*;

use crate::search::indexers::index_activity_from_data_room_publication_record;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeActivityMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_MOLECULE_ACTIVITY_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct MoleculeActivitySearchUpdater {
    search_service: Arc<dyn SearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeActivitySearchUpdater {
    async fn handle_published_message(
        &self,
        ctx: SearchContext<'_>,
        published_message: &MoleculeActivityMessagePublished,
    ) -> Result<(), InternalError> {
        let activity_document = index_activity_from_data_room_publication_record(
            &published_message.molecule_account_id,
            &published_message.activity_record,
            published_message.event_time,
            published_message.offset,
        );

        let id = activity_schema::unique_id_for_data_room_activity(
            &published_message.molecule_account_id,
            published_message.offset,
        );

        self.search_service
            .bulk_update(
                ctx,
                activity_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Index {
                    id,
                    doc: activity_document,
                }],
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MoleculeActivitySearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeActivityMessage> for MoleculeActivitySearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeActivitySearchUpdater[MoleculeActivityMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &MoleculeActivityMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule activity message");

        let ctx = SearchContext::unrestricted(target_catalog);

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
