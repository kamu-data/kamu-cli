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
    ResourceLifecycleMessage,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Hints the controller runtime of every change to a resource, except a
/// completed deletion. The controllers find the work themselves: a hint only
/// wakes the loop of the resource's type.
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
        _target_catalog: &Catalog,
        message: &ResourceLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received resource lifecycle message");

        match message {
            ResourceLifecycleMessage::Applied(applied_message) => {
                self.hints.hint(&applied_message.resource.schema);
            }
            ResourceLifecycleMessage::DeletionRequested(requested_message) => {
                debug_assert!(
                    !requested_message.resources.is_empty(),
                    "deletion requested message must contain at least one resource"
                );
                self.hints.hint(&requested_message.resources[0].schema);
            }
            ResourceLifecycleMessage::StatusUpdated(status_message) => {
                self.hints.hint(&status_message.resource.schema);
            }
            // Nothing is left to do for a resource whose deletion completed
            ResourceLifecycleMessage::Deleted(_) => {}
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
