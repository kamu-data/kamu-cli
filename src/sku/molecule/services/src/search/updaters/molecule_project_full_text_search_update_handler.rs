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
use kamu_search::{FullTextSearchContext, FullTextSearchService, FullTextUpdateOperation};
use messaging_outbox::{MessageConsumer, MessageConsumerT};

use crate::domain::{
    MoleculeProjectMessage,
    MoleculeProjectMessageCreated,
    MoleculeProjectMessageDisabled,
    MoleculeProjectMessageReenabled,
    molecule_project_full_text_search_schema as project_schema,
};
use crate::search::molecule_full_text_search_schema_helpers as schema_helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeProjectMessage>)]
pub struct MoleculeProjectFullTextSearchUpdateHandler {
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeProjectFullTextSearchUpdateHandler {
    async fn handle_created_message(
        &self,
        ctx: FullTextSearchContext<'_>,
        created_message: &MoleculeProjectMessageCreated,
    ) -> Result<(), InternalError> {
        let project_document = schema_helpers::index_project_from_parts(
            &created_message.ipnft_uid,
            &created_message.ipnft_symbol,
            &created_message.project_account_id,
            created_message.event_time,
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
        let partial_update = schema_helpers::partial_update_project_when_ban_status_changed(
            true,
            disabled_message.event_time,
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
        let partial_update = schema_helpers::partial_update_project_when_ban_status_changed(
            false,
            reenabled_message.event_time,
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

impl MessageConsumer for MoleculeProjectFullTextSearchUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeProjectMessage> for MoleculeProjectFullTextSearchUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeProjectFullTextSearchUpdateHandler[MoleculeProjectMessage]"
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
