// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface, Catalog};
use internal_error::InternalError;
use kamu_core::{MESSAGE_KAMU_CORE_DATASET_DELETED, MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE};
use kamu_flow_system::{
    FlowConfigurationUpdatedMessage,
    FlowServiceUpdatedMessage,
    MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED,
    MESSAGE_KAMU_FLOW_SERVICE_UPDATED,
};
use kamu_task_system::{
    MESSAGE_KAMU_TASK_FINISHED,
    MESSAGE_KAMU_TASK_RUNNING,
    MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
};
use messaging_outbox::{
    consume_deserialized_message,
    ConsumerFilter,
    MessageConsumerMetaInfo,
    MessageConsumerMetaInfoRecord,
    MessageConsumersMediator,
};

use crate::{
    MESSAGE_CONSUMER_KAMU_FLOW_CONFIGURATION_SERVICE,
    MESSAGE_CONSUMER_KAMU_FLOW_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumersMediator)]
pub struct FlowMessageConsumerMediator {}

#[async_trait::async_trait]
impl MessageConsumersMediator for FlowMessageConsumerMediator {
    fn get_supported_message_types(&self) -> &[&'static str] {
        &[
            // Flow configuration service producer
            MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED,
            // Flow service producer
            MESSAGE_KAMU_FLOW_SERVICE_UPDATED,
        ]
    }

    fn get_consumers_meta_info(&self) -> Vec<MessageConsumerMetaInfo> {
        vec![
            MessageConsumerMetaInfo {
                consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_CONFIGURATION_SERVICE,
                records: vec![MessageConsumerMetaInfoRecord {
                    producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                    message_type: MESSAGE_KAMU_CORE_DATASET_DELETED,
                }],
            },
            MessageConsumerMetaInfo {
                consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_SERVICE,
                records: vec![
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                        message_type: MESSAGE_KAMU_CORE_DATASET_DELETED,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
                        message_type: MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                        message_type: MESSAGE_KAMU_TASK_RUNNING,
                    },
                    MessageConsumerMetaInfoRecord {
                        producer_name: MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                        message_type: MESSAGE_KAMU_TASK_FINISHED,
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
            MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED => {
                consume_deserialized_message::<FlowConfigurationUpdatedMessage>(
                    catalog,
                    consumer_filter,
                    message_type,
                    content_json,
                )
                .await
            }

            MESSAGE_KAMU_FLOW_SERVICE_UPDATED => {
                consume_deserialized_message::<FlowServiceUpdatedMessage>(
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
