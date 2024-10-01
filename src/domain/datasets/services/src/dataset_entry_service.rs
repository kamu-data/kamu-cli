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
use kamu_core::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionDurability,
};
use time_source::SystemTimeSource;

use crate::MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
    ],
    durability: MessageConsumptionDurability::Durable,
})]
pub struct DatasetEntryService {
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl DatasetEntryService {
    pub fn new(
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_entry_repo,
            time_source,
        }
    }

    async fn handle_dataset_lifecycle_created_message(
        &self,
        message @ DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
            ..
        }: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let entry = DatasetEntry::new(
            dataset_id.clone(),
            owner_account_id.clone(),
            message.dataset_name().into_owned(),
            self.time_source.now(),
        );

        self.dataset_entry_repo
            .save_dataset_entry(&entry)
            .await
            .int_err()
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        DatasetLifecycleMessageDeleted { dataset_id, .. }: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        self.dataset_entry_repo
            .delete_dataset_entry(dataset_id)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetEntryService {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetEntryService {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetEntryService[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        // TODO: handle rename
        match message {
            DatasetLifecycleMessage::Created(message) => {
                self.handle_dataset_lifecycle_created_message(message).await
            }

            DatasetLifecycleMessage::Deleted(message) => {
                self.handle_dataset_lifecycle_deleted_message(message).await
            }

            DatasetLifecycleMessage::DependenciesUpdated(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
