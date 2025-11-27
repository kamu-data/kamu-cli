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
    AccountLifecycleMessageUpdated,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
};
use kamu_datasets::*;
use messaging_outbox::prelude::*;

use crate::DatasetEntryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASETS_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetAccountLifecycleHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    delete_dataset_use_case: Arc<dyn DeleteDatasetUseCase>,
}

impl DatasetAccountLifecycleHandler {
    async fn handle_account_lifecycle_updated_message(
        &self,
        message: &AccountLifecycleMessageUpdated,
    ) -> Result<(), InternalError> {
        // Only react to account renames
        if message.old_account_name != message.new_account_name {
            self.dataset_entry_writer
                .update_owner_entries_after_rename(
                    &message.account_id,
                    &message.old_account_name,
                    &message.new_account_name,
                )
                .await?;
        }

        Ok(())
    }

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
                .execute_via_handle_preauthorized(&dataset_handle)
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

impl MessageConsumer for DatasetAccountLifecycleHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DatasetAccountLifecycleHandler {
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
            AccountLifecycleMessage::Updated(message) => {
                self.handle_account_lifecycle_updated_message(message).await
            }

            AccountLifecycleMessage::Deleted(message) => {
                self.handle_account_lifecycle_deleted_message(message).await
            }

            AccountLifecycleMessage::Created(_) | AccountLifecycleMessage::PasswordChanged(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
