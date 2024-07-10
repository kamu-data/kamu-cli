// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface, Catalog};
use kamu_core::{
    DatasetCreatedMessage,
    DatasetDeletedMessage,
    DatasetDependenciesUpdatedMessage,
    InternalError,
    MESSAGE_CONSUMER_KAMU_CORE_DATASET_OWNERSHIP_SERVICE,
    MESSAGE_CONSUMER_KAMU_CORE_DEPENDENCY_GRAPH_SERVICE,
    MESSAGE_KAMU_CORE_DATASET_CREATED,
    MESSAGE_KAMU_CORE_DATASET_DELETED,
    MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{
    consume_deserialized_message,
    ConsumerFilter,
    MessageConsumerMetaInfo,
    MessageConsumerMetaInfoRecord,
    MessageConsumersMediator,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumersMediator)]
pub struct CoreMessageConsumerMediator {}

#[async_trait::async_trait]
impl MessageConsumersMediator for CoreMessageConsumerMediator {
    fn get_supported_message_types(&self) -> &[&'static str] {
        &[
            MESSAGE_KAMU_CORE_DATASET_CREATED,
            MESSAGE_KAMU_CORE_DATASET_DELETED,
            MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED,
        ]
    }

    fn get_consumers_meta_info(&self) -> Vec<MessageConsumerMetaInfo> {
        vec![
            MessageConsumerMetaInfo {
                consumer_name: MESSAGE_CONSUMER_KAMU_CORE_DATASET_OWNERSHIP_SERVICE,
                records: vec![
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_CREATED,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_DELETED,
                    },
                ],
            },
            MessageConsumerMetaInfo {
                consumer_name: MESSAGE_CONSUMER_KAMU_CORE_DEPENDENCY_GRAPH_SERVICE,
                records: vec![
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_CREATED,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_DELETED,
                    },
                ],
            },
        ]
    }

    async fn consume_message<'a>(
        &self,
        catalog: &Catalog,
        consumer_filter: ConsumerFilter<'a>,
        message_type: &str,
        content_json: serde_json::Value,
    ) -> Result<(), InternalError> {
        match message_type {
            MESSAGE_KAMU_CORE_DATASET_CREATED => {
                consume_deserialized_message::<DatasetCreatedMessage>(
                    catalog,
                    consumer_filter,
                    message_type,
                    content_json,
                )
                .await
            }

            MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED => {
                consume_deserialized_message::<DatasetDependenciesUpdatedMessage>(
                    catalog,
                    consumer_filter,
                    message_type,
                    content_json,
                )
                .await
            }

            MESSAGE_KAMU_CORE_DATASET_DELETED => {
                consume_deserialized_message::<DatasetDeletedMessage>(
                    catalog,
                    consumer_filter,
                    message_type,
                    content_json,
                )
                .await
            }

            _ => panic!("Unsupported {message_type}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
