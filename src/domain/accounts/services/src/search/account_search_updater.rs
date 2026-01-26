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
use kamu_accounts::*;
use kamu_search::*;
use messaging_outbox::*;

use crate::search::account_search_indexer::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn messaging_outbox::MessageConsumer)]
#[dill::interface(dyn messaging_outbox::MessageConsumerT<AccountLifecycleMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_ACCOUNTS_SEARCH_UPDATER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct AccountSearchUpdater {
    search_service: Arc<dyn SearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AccountSearchUpdater {
    async fn handle_new_account(
        &self,
        catalog: &dill::Catalog,
        new_account_message: &AccountLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let doc = index_from_parts(
            &new_account_message.account_name,
            &new_account_message.display_name,
            new_account_message.event_time,
        );

        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                account_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Index {
                    id: new_account_message.account_id.to_string(),
                    doc,
                }],
            )
            .await
            .int_err()
    }

    async fn handle_updated_account(
        &self,
        catalog: &dill::Catalog,
        updated_account_message: &AccountLifecycleMessageUpdated,
    ) -> Result<(), InternalError> {
        // Only update search index if account_name or display_name changed
        // We don't care about email changes for search
        let name_changed =
            updated_account_message.old_account_name != updated_account_message.new_account_name;
        let display_name_changed =
            updated_account_message.old_display_name != updated_account_message.new_display_name;

        if !name_changed && !display_name_changed {
            // No fields relevant to search were updated
            return Ok(());
        }

        let partial_update = partial_update_for_account(
            &updated_account_message.new_account_name,
            &updated_account_message.new_display_name,
            updated_account_message.event_time,
        );

        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                account_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Update {
                    id: updated_account_message.account_id.to_string(),
                    doc: partial_update,
                }],
            )
            .await
            .int_err()
    }

    async fn handle_deleted_account(
        &self,
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
    ) -> Result<(), InternalError> {
        self.search_service
            .bulk_update(
                SearchContext::unrestricted(catalog),
                account_search_schema::SCHEMA_NAME,
                vec![SearchIndexUpdateOperation::Delete {
                    id: account_id.to_string(),
                }],
            )
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for AccountSearchUpdater {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for AccountSearchUpdater {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "AccountSearchUpdater[AccountLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received account lifecycle message");

        match message {
            AccountLifecycleMessage::Created(new_account_message) => {
                self.handle_new_account(target_catalog, new_account_message)
                    .await?;
            }
            AccountLifecycleMessage::Updated(updated_account_message) => {
                self.handle_updated_account(target_catalog, updated_account_message)
                    .await?;
            }
            AccountLifecycleMessage::Deleted(deleted_account_message) => {
                self.handle_deleted_account(target_catalog, &deleted_account_message.account_id)
                    .await?;
            }

            AccountLifecycleMessage::PasswordChanged(_) => {
                // No-op for search
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
