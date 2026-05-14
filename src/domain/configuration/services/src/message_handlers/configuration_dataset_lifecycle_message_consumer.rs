// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, component, interface, meta};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_configuration::{DatasetSecretSetBindingRepository, DatasetVariableSetBindingRepository};
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MESSAGE_CONSUMER_KAMU_CONFIGURATION_DATASET_LIFECYCLE_HANDLER: &str =
    "dev.kamu.domain.configuration.ConfigurationDatasetLifecycleHandler";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_CONFIGURATION_DATASET_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct ConfigurationDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for ConfigurationDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for ConfigurationDatasetLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "ConfigurationDatasetLifecycleMessageConsumer[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        let DatasetLifecycleMessage::Deleted(deleted_message) = message else {
            return Ok(());
        };

        // --- Delete all variable set bindings for the dataset

        let variable_repo = target_catalog
            .get_one::<dyn DatasetVariableSetBindingRepository>()
            .map_err(ErrorIntoInternal::int_err)?;

        variable_repo
            .delete_bindings_for_dataset(&deleted_message.dataset_id)
            .await?;

        // --- Delete all secret set bindings for the dataset

        let secret_repo = target_catalog
            .get_one::<dyn DatasetSecretSetBindingRepository>()
            .map_err(ErrorIntoInternal::int_err)?;

        secret_repo
            .delete_bindings_for_dataset(&deleted_message.dataset_id)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
