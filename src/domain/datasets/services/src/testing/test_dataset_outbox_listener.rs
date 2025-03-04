// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetReferenceMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestDatasetOutboxListener {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    dataset_lifecycle_messages: Vec<DatasetLifecycleMessage>,
    dataset_reference_messages: Vec<DatasetReferenceMessage>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "TestOutboxDispatcher",
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
})]
impl TestDatasetOutboxListener {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }
}

impl MessageConsumer for TestDatasetOutboxListener {}

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for TestDatasetOutboxListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.dataset_lifecycle_messages.push(message.clone());
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for TestDatasetOutboxListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.dataset_reference_messages.push(message.clone());
        Ok(())
    }
}

impl std::fmt::Display for TestDatasetOutboxListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap();

        writeln!(
            f,
            "Dataset Lifecycle Messages: {}",
            guard.dataset_lifecycle_messages.len()
        )?;

        for msg in &guard.dataset_lifecycle_messages {
            match msg {
                DatasetLifecycleMessage::Created(created_msg) => {
                    writeln!(f, "  Created {{",)?;
                    writeln!(f, "    Dataset ID: {}", created_msg.dataset_id)?;
                    writeln!(f, "    Dataset Name: {}", created_msg.dataset_name)?;
                    writeln!(f, "    Owner: {}", created_msg.owner_account_id)?;
                    writeln!(f, "    Visibility: {}", created_msg.dataset_visibility)?;
                    writeln!(f, "  }}")?;
                }
                DatasetLifecycleMessage::Deleted(deleted_msg) => {
                    writeln!(f, "  Deleted {{",)?;
                    writeln!(f, "    Dataset ID: {}", deleted_msg.dataset_id)?;
                    writeln!(f, "  }}")?;
                }
            }
        }

        writeln!(
            f,
            "Dataset Reference Messages: {}",
            guard.dataset_reference_messages.len()
        )?;

        for msg in &guard.dataset_reference_messages {
            match msg {
                DatasetReferenceMessage::Updated(updated_ref_msg) => {
                    writeln!(f, "  Ref Updated {{",)?;
                    writeln!(f, "    Dataset ID: {}", updated_ref_msg.dataset_id)?;
                    writeln!(f, "    Ref: {}", updated_ref_msg.block_ref)?;
                    writeln!(
                        f,
                        "    Prev Head: {:?}",
                        updated_ref_msg.maybe_prev_block_hash
                    )?;
                    writeln!(f, "    New Head: {:?}", updated_ref_msg.new_block_hash)?;
                    writeln!(f, "  }}")?;
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
