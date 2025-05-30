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
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageDeleted,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[dill::interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_ACCOUNTS_DID_SECRET_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    // NOTE: To clean up database rows in a single transaction, process the message immediately
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DidSecretService {
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DidSecretService {
    async fn handle_account_lifecycle_deleted_message(
        &self,
        message: &AccountLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        use odf::metadata::AsStackString;

        let account_id = message.account_id.as_stack_string();
        let account_entity = DidEntity::new_account(account_id.as_str());

        match self
            .did_secret_key_repo
            .delete_did_secret_key(&account_entity)
            .await
        {
            Ok(_) | Err(DeleteDidSecretKeyError::NotFound { .. }) => { /* idempotent deletion */ }
            e @ Err(_) => e.int_err()?,
        }

        Ok(())
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        message: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        let dataset_id = message.dataset_id.as_did_str().to_stack_string();
        let dataset_entity = DidEntity::new_dataset(dataset_id.as_str());

        match self
            .did_secret_key_repo
            .delete_did_secret_key(&dataset_entity)
            .await
        {
            Ok(_) | Err(DeleteDidSecretKeyError::NotFound { .. }) => { /* idempotent deletion */ }
            e @ Err(_) => e.int_err()?,
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DidSecretService {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DidSecretService {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DidSecretService[AccountLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received account lifecycle message");

        match message {
            AccountLifecycleMessage::Deleted(message) => {
                self.handle_account_lifecycle_deleted_message(message).await
            }

            AccountLifecycleMessage::Created(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DidSecretService {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DidSecretService[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Deleted(message) => {
                self.handle_dataset_lifecycle_deleted_message(message).await
            }

            DatasetLifecycleMessage::Created(_) | DatasetLifecycleMessage::Renamed(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
