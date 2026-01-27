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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::{
    DatasetProperties,
    DeleteDatasetRebacPropertiesUseCase,
    MESSAGE_CONSUMER_KAMU_REBAC_SERVICE,
    SetDatasetRebacPropertiesUseCase,
};
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

use crate::DefaultDatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_REBAC_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct RebacDatasetLifecycleMessageConsumer {
    set_dataset_properties_uc: Arc<dyn SetDatasetRebacPropertiesUseCase>,
    delete_dataset_properties_uc: Arc<dyn DeleteDatasetRebacPropertiesUseCase>,
    default_dataset_properties: Arc<DefaultDatasetProperties>,
}

impl RebacDatasetLifecycleMessageConsumer {
    async fn handle_dataset_lifecycle_created_message(
        &self,
        message: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let dataset_properties = DatasetProperties {
            allows_public_read: message.dataset_visibility.is_public(),
            allows_anonymous_read: self.default_dataset_properties.allows_anonymous_read,
        };

        self.set_dataset_properties_uc
            .execute(&message.dataset_id, dataset_properties)
            .await
            .int_err()?;

        Ok(())
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        message: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        self.delete_dataset_properties_uc
            .execute(&message.dataset_id)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for RebacDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for RebacDatasetLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "RebacDatasetLifecycleMessageConsumer[DatasetLifecycleMessage]"
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

            // No action required
            DatasetLifecycleMessage::Renamed(_) => Ok(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
