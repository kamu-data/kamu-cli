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
use kamu_accounts::{
    AccountLifecycleMessage,
    AccountLifecycleMessageDeleted,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
};
use kamu_core::DatasetRegistry;
use kamu_datasets::*;
use messaging_outbox::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASETS_DELETION_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    // NOTE: To clean up database rows in a single transaction, process the message immediately
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetAccountDeletionHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset_use_case: Arc<dyn DeleteDatasetUseCase>,
}

impl DatasetAccountDeletionHandler {
    async fn handle_account_lifecycle_deleted_message(
        &self,
        message: &AccountLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        let mut owned_dataset_stream = self
            .dataset_registry
            .all_dataset_handles_by_owner_id(&message.account_id);

        use tokio_stream::StreamExt;

        // TODO: PERF: Batch/concurrent processing
        while let Some(dataset_handle) = owned_dataset_stream.try_next().await? {
            match self
                .delete_dataset_use_case
                .execute_via_handle(&dataset_handle)
                .await
            {
                Ok(_) | Err(DeleteDatasetError::NotFound(_)) => { /* idempotent deletion */ }
                e @ Err(_) => e.int_err()?,
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetAccountDeletionHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DatasetAccountDeletionHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetAccountDeletionHandler[AccountLifecycleMessage]"
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
