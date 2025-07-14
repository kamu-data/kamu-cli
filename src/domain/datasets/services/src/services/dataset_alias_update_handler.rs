// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_accounts::{AccountLifecycleMessage, MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE};
use kamu_core::{DatasetRegistry, DatasetRegistryExt};
use kamu_datasets::{
    DatasetLifecycleMessage,
    MESSAGE_CONSUMER_KAMU_DATASET_ALIAS_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_ALIAS_UPDATE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    // Only write aliases after reference transaction succeeds!
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetAliasUpdateHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetAliasUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetAliasUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetAliasUpdateHandler[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        // Potential extensions:
        //  - dataset transfer to another owner

        match message {
            // Only react to renaming, creation writes alias immediately
            DatasetLifecycleMessage::Renamed(renamed_message) => {
                let target = match self
                    .dataset_registry
                    .get_dataset_by_id(&renamed_message.dataset_id)
                    .await
                {
                    Ok(target) => Ok(target),
                    Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
                        tracing::error!(
                            %renamed_message.dataset_id, err = ?e,
                            "Writing dataset alias skipped. Dataset not found."
                        );
                        return Ok(());
                    }
                    Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
                }?;

                odf::dataset::write_dataset_alias(
                    target.as_ref(),
                    &odf::DatasetAlias::new(
                        target.get_alias().account_name.clone(),
                        renamed_message.new_dataset_name.clone(),
                    ),
                )
                .await
            }

            DatasetLifecycleMessage::Created(_) | DatasetLifecycleMessage::Deleted(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DatasetAliasUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetAliasUpdateHandler[AccountLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received account lifecycle message");

        match message {
            AccountLifecycleMessage::Renamed(renamed_message) => {
                // Update dataset aliases for all datasets owned by the renamed account
                let mut owned_dataset_stream = self
                    .dataset_registry
                    .all_dataset_handles_by_owner_id(&renamed_message.account_id);

                use tokio_stream::StreamExt;
                while let Some(dataset_handle) = owned_dataset_stream.try_next().await? {
                    // Resolve the dataset handle
                    let target = self
                        .dataset_registry
                        .get_dataset_by_handle(&dataset_handle)
                        .await;

                    // Write the updated alias
                    odf::dataset::write_dataset_alias(
                        target.as_ref(),
                        &odf::DatasetAlias::new(
                            Some(renamed_message.new_account_name.clone()),
                            dataset_handle.alias.dataset_name,
                        ),
                    )
                    .await?;
                }
                Ok(())
            }

            AccountLifecycleMessage::Created(_)
            | AccountLifecycleMessage::PasswordChanged(_)
            | AccountLifecycleMessage::Deleted(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
