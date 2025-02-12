// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta, Catalog};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::{DatasetPropertyName, RebacService};
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

use crate::{RebacServiceImpl, MESSAGE_CONSUMER_KAMU_REBAC_SERVICE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MultiTenantRebacDatasetLifecycleMessageConsumer {
    rebac_service: Arc<RebacServiceImpl>,
}

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_REBAC_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
})]
impl MultiTenantRebacDatasetLifecycleMessageConsumer {
    pub fn new(rebac_service: Arc<RebacServiceImpl>) -> Self {
        Self { rebac_service }
    }

    async fn handle_dataset_lifecycle_created_message(
        &self,
        message: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let allows = message.dataset_visibility.is_public();

        for (name, value) in [
            DatasetPropertyName::allows_public_read(allows),
            // TODO: Private Datasets: Read from a specific environment's config
            DatasetPropertyName::allows_anonymous_read(allows),
        ] {
            self.rebac_service
                .set_dataset_property(&message.dataset_id, name, &value)
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        message: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        self.rebac_service
            .delete_dataset_properties(&message.dataset_id)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for MultiTenantRebacDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for MultiTenantRebacDatasetLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MultiTenantRebacDatasetLifecycleMessageConsumer[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Created(message) => {
                self.handle_dataset_lifecycle_created_message(message).await
            }

            DatasetLifecycleMessage::Deleted(message) => {
                self.handle_dataset_lifecycle_deleted_message(message).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
