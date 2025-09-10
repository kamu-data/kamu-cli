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
    DatasetDependenciesMessage,
    DatasetKeyBlocksMessage,
    DatasetLifecycleMessage,
    DatasetReferenceMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
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
    dataset_dependency_messages: Vec<DatasetDependenciesMessage>,
    dataset_key_blocks_messages: Vec<DatasetKeyBlocksMessage>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[interface(dyn MessageConsumerT<DatasetDependenciesMessage>)]
#[interface(dyn MessageConsumerT<DatasetKeyBlocksMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "TestOutboxDispatcher",
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
impl TestDatasetOutboxListener {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    pub fn reset(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.dataset_lifecycle_messages.clear();
        guard.dataset_reference_messages.clear();
        guard.dataset_dependency_messages.clear();
        guard.dataset_key_blocks_messages.clear();
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

#[async_trait::async_trait]
impl MessageConsumerT<DatasetDependenciesMessage> for TestDatasetOutboxListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetDependenciesMessage,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.dataset_dependency_messages.push(message.clone());
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageConsumerT<DatasetKeyBlocksMessage> for TestDatasetOutboxListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetKeyBlocksMessage,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.dataset_key_blocks_messages.push(message.clone());
        Ok(())
    }
}

impl std::fmt::Display for TestDatasetOutboxListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap();

        if !guard.dataset_lifecycle_messages.is_empty() {
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
                    DatasetLifecycleMessage::Renamed(renamed_msg) => {
                        writeln!(f, "  Renamed {{",)?;
                        writeln!(f, "    Dataset ID: {}", renamed_msg.dataset_id)?;
                        writeln!(f, "    New Name: {}", renamed_msg.new_dataset_name)?;
                        writeln!(f, "  }}")?;
                    }
                    DatasetLifecycleMessage::Deleted(deleted_msg) => {
                        writeln!(f, "  Deleted {{",)?;
                        writeln!(f, "    Dataset ID: {}", deleted_msg.dataset_id)?;
                        writeln!(f, "  }}")?;
                    }
                }
            }
        }

        if !guard.dataset_reference_messages.is_empty() {
            writeln!(
                f,
                "Dataset Reference Messages: {}",
                guard.dataset_reference_messages.len()
            )?;

            for msg in &guard.dataset_reference_messages {
                match msg {
                    DatasetReferenceMessage::Updated(msg) => {
                        writeln!(f, "  Ref Updated {{",)?;
                        writeln!(f, "    Dataset ID: {}", msg.dataset_id)?;
                        writeln!(f, "    Ref: {}", msg.block_ref)?;
                        writeln!(f, "    Prev Head: {:?}", msg.maybe_prev_block_hash)?;
                        writeln!(f, "    New Head: {:?}", msg.new_block_hash)?;
                        writeln!(f, "  }}")?;
                    }
                }
            }
        }

        if !guard.dataset_dependency_messages.is_empty() {
            writeln!(
                f,
                "Dataset Dependency Messages: {}",
                guard.dataset_dependency_messages.len()
            )?;

            for msg in &guard.dataset_dependency_messages {
                match msg {
                    DatasetDependenciesMessage::Updated(msg) => {
                        writeln!(f, "  Deps Updated {{",)?;
                        writeln!(f, "    Dataset ID: {}", msg.dataset_id)?;
                        writeln!(
                            f,
                            "    Added: [{}]",
                            msg.added_upstream_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )?;
                        writeln!(
                            f,
                            "    Removed: [{}]",
                            msg.removed_upstream_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )?;
                        writeln!(f, "  }}")?;
                    }
                }
            }
        }

        if !guard.dataset_key_blocks_messages.is_empty() {
            writeln!(
                f,
                "Dataset Key Block Messages: {}",
                guard.dataset_key_blocks_messages.len()
            )?;

            for msg in &guard.dataset_key_blocks_messages {
                match msg {
                    DatasetKeyBlocksMessage::Appended(msg) => {
                        writeln!(f, "  Key Blocks Appended {{",)?;
                        writeln!(f, "    Dataset ID: {}", msg.dataset_id)?;
                        writeln!(f, "    Ref: {}", msg.block_ref)?;
                        writeln!(f, "    Key Block Tail: {}", msg.tail_key_block_hash)?;
                        writeln!(f, "    Key Block Head: {}", msg.head_key_block_hash)?;
                        writeln!(f, "  }}")?;
                    }
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
