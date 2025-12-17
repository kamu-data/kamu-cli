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
    MESSAGE_CONSUMER_MOLECULE_PROJECT_SEARCH_UPDATER,
    MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
    MoleculeProjectMessage,
    MoleculeProjectMessageCreated,
    MoleculeProjectMessageDisabled,
    MoleculeProjectMessageReenabled,
    molecule_project_full_text_search_schema as project_schema,
};
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::*;

use crate::search::indexers::{
    index_project_from_parts,
    partial_update_project_when_ban_status_changed,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeProjectMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_MOLECULE_PROJECT_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct MoleculeProjectSearchUpdater {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeProjectSearchUpdater {
    async fn handle_created_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        created_message: &MoleculeProjectMessageCreated,
    ) -> Result<(), InternalError> {
        let project_document = index_project_from_parts(
            &created_message.molecule_account_id,
            &created_message.ipnft_uid,
            &created_message.ipnft_symbol,
            &created_message.project_account_id,
            created_message.event_time,
            created_message.system_time,
        );

        self.full_text_search_service
            .bulk_update(
                ctx,
                project_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Index {
                    id: created_message.ipnft_uid.clone(),
                    doc: project_document,
                }],
            )
            .await?;

        Ok(())
    }

    async fn handle_disabled_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        disabled_message: &MoleculeProjectMessageDisabled,
    ) -> Result<(), InternalError> {
        let partial_update = partial_update_project_when_ban_status_changed(
            true,
            disabled_message.event_time,
            disabled_message.system_time,
        );
        self.full_text_search_service
            .bulk_update(
                ctx,
                project_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Update {
                    id: disabled_message.ipnft_uid.clone(),
                    doc: partial_update,
                }],
            )
            .await?;

        Ok(())
    }

    async fn handle_reenabled_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        reenabled_message: &MoleculeProjectMessageReenabled,
    ) -> Result<(), InternalError> {
        let partial_update = partial_update_project_when_ban_status_changed(
            false,
            reenabled_message.event_time,
            reenabled_message.system_time,
        );
        self.full_text_search_service
            .bulk_update(
                ctx,
                project_schema::SCHEMA_NAME,
                vec![FullTextUpdateOperation::Update {
                    id: reenabled_message.ipnft_uid.clone(),
                    doc: partial_update,
                }],
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MoleculeProjectSearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeProjectMessage> for MoleculeProjectSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeProjectSearchUpdater[MoleculeProjectMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &MoleculeProjectMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule project message");

        let ctx = FullTextSearchContext {
            catalog: target_catalog,
        };

        match message {
            MoleculeProjectMessage::Created(created_message) => {
                self.handle_created_message(ctx, created_message).await?;
            }
            MoleculeProjectMessage::Disabled(disabled_message) => {
                self.handle_disabled_message(ctx, disabled_message).await?;
            }
            MoleculeProjectMessage::Reenabled(reenabled_message) => {
                self.handle_reenabled_message(ctx, reenabled_message)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
