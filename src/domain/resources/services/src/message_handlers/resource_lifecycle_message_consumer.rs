// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, component, interface, meta};
use internal_error::InternalError;
use kamu_resources::{
    MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceControllerHints,
    ResourceLifecycleEventDispatcher,
    ResourceLifecycleMessage,
    TypeUri,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};

use crate::ResourceDispatcherFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Turns lifecycle messages into work for the controllers of their type.
///
/// Every change but a completed deletion hints the controller runtime. A type
/// with a lifecycle dispatcher is also reconciled by it, once per message; a
/// type without one is left to its `ResourceController`.
#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<ResourceLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalSelfManaged,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct ResourceLifecycleMessageConsumer {
    hints: Arc<dyn ResourceControllerHints>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for ResourceLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<ResourceLifecycleMessage> for ResourceLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "ResourceLifecycleMessageConsumer[ResourceLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &ResourceLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received resource lifecycle message");

        match message {
            ResourceLifecycleMessage::Applied(applied_message) => {
                let resource = &applied_message.resource;
                self.hints.hint(&resource.schema);

                let Some(dispatcher) = Self::find_dispatcher(target_catalog, &resource.schema)?
                else {
                    return Ok(());
                };

                dispatcher.handle_applied(resource).await
            }
            ResourceLifecycleMessage::DeletionRequested(requested_message) => {
                debug_assert!(
                    !requested_message.resources.is_empty(),
                    "deletion requested message must contain at least one resource"
                );

                let schema = &requested_message.resources[0].schema;
                self.hints.hint(schema);

                let Some(dispatcher) = Self::find_dispatcher(target_catalog, schema)? else {
                    return Ok(());
                };

                dispatcher
                    .handle_deletion_requested(&requested_message.resources)
                    .await
            }
            ResourceLifecycleMessage::Deleted(deleted_message) => {
                debug_assert!(
                    !deleted_message.resources.is_empty(),
                    "deleted message must contain at least one resource"
                );

                let schema = &deleted_message.resources[0].schema;
                let Some(dispatcher) = Self::find_dispatcher(target_catalog, schema)? else {
                    return Ok(());
                };

                for resource in &deleted_message.resources {
                    dispatcher.handle_deleted(resource).await?;
                }

                Ok(())
            }
            // The one-shot dispatch does not react to status changes, but a
            // controller may
            ResourceLifecycleMessage::StatusUpdated(status_message) => {
                self.hints.hint(&status_message.resource.schema);
                Ok(())
            }
        }
    }
}

impl ResourceLifecycleMessageConsumer {
    /// The type's lifecycle dispatcher, if it has one. A type without one is
    /// left to its controller; one without a controller either is reconciled
    /// by nothing, which only this log shows.
    fn find_dispatcher(
        target_catalog: &Catalog,
        schema: &TypeUri,
    ) -> Result<Option<Arc<dyn ResourceLifecycleEventDispatcher>>, InternalError> {
        let dispatcher =
            ResourceDispatcherFactory::find_lifecycle_dispatcher_in(target_catalog, schema)?;
        if dispatcher.is_none() {
            tracing::debug!(%schema, "No lifecycle dispatcher, leaving the resource to its controller");
        }
        Ok(dispatcher)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
