// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, component, interface, meta};
use internal_error::InternalError;
use kamu_resources::{
    MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceLifecycleMessage,
    get_resource_lifecycle_dispatcher_from_catalog,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<ResourceLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct ResourceLifecycleMessageConsumer {}

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
                let dispatcher = get_resource_lifecycle_dispatcher_from_catalog(
                    target_catalog,
                    &applied_message.resource,
                )?;

                dispatcher.handle_applied(&applied_message.resource).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
