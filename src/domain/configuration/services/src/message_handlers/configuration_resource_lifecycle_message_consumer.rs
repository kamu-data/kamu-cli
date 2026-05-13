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
use kamu_configuration::{
    SecretSetProjectionRepository,
    SecretSetResource,
    VariableSetProjectionRepository,
    VariableSetResource,
};
use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MESSAGE_CONSUMER_KAMU_CONFIGURATION_RESOURCE_LIFECYCLE_HANDLER: &str =
    "dev.kamu.domain.configuration.ConfigurationResourceLifecycleHandler";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<ResourceLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_CONFIGURATION_RESOURCE_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct ConfigurationResourceLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for ConfigurationResourceLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<ResourceLifecycleMessage> for ConfigurationResourceLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "ConfigurationResourceLifecycleMessageConsumer[ResourceLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &ResourceLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received resource lifecycle message");

        match message {
            ResourceLifecycleMessage::ReconciliationSucceeded(succeeded_message) => {
                match succeeded_message.resource.kind.as_str() {
                    VariableSetResource::RESOURCE_TYPE => {
                        let repo = target_catalog
                            .get_one::<dyn VariableSetProjectionRepository>()
                            .map_err(ErrorIntoInternal::int_err)?;

                        repo.cleanup_entries_before_generation(
                            &succeeded_message.resource.uid,
                            succeeded_message.resource.metadata.generation,
                        )
                        .await
                    }
                    SecretSetResource::RESOURCE_TYPE => {
                        let repo = target_catalog
                            .get_one::<dyn SecretSetProjectionRepository>()
                            .map_err(ErrorIntoInternal::int_err)?;

                        repo.cleanup_entries_before_generation(
                            &succeeded_message.resource.uid,
                            succeeded_message.resource.metadata.generation,
                        )
                        .await
                    }
                    _ => Ok(()),
                }
            }
            ResourceLifecycleMessage::Deleted(deleted_message) => {
                match deleted_message.resource.kind.as_str() {
                    VariableSetResource::RESOURCE_TYPE => {
                        let repo = target_catalog
                            .get_one::<dyn VariableSetProjectionRepository>()
                            .map_err(ErrorIntoInternal::int_err)?;

                        repo.delete_all_entries(&deleted_message.resource.uid).await
                    }
                    SecretSetResource::RESOURCE_TYPE => {
                        let repo = target_catalog
                            .get_one::<dyn SecretSetProjectionRepository>()
                            .map_err(ErrorIntoInternal::int_err)?;

                        repo.delete_all_entries(&deleted_message.resource.uid).await
                    }
                    _ => Ok(()),
                }
            }

            ResourceLifecycleMessage::Applied(_)
            | ResourceLifecycleMessage::ReconciliationFailed(_) => {
                // Nothing to do here
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
