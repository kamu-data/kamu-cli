// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface, Catalog};
use kamu_core::InternalError;
use kamu_task_system::{
    TaskFinishedMessage,
    TaskRunningMessage,
    MESSAGE_KAMU_TASK_FINISHED,
    MESSAGE_KAMU_TASK_RUNNING,
};
use messaging_outbox::{
    consume_deserialized_message,
    ConsumerFilter,
    MessageConsumerMetaInfo,
    MessageConsumersMediator,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumersMediator)]
pub struct TaskMessageConsumerMediator {}

#[async_trait::async_trait]
impl MessageConsumersMediator for TaskMessageConsumerMediator {
    fn get_supported_message_types(&self) -> &[&'static str] {
        &[
            // Task executor producer
            MESSAGE_KAMU_TASK_RUNNING,
            MESSAGE_KAMU_TASK_FINISHED,
        ]
    }

    fn get_consumers_meta_info(&self) -> Vec<MessageConsumerMetaInfo> {
        vec![] // No consumers in this domain
    }

    async fn consume_message<'a>(
        &self,
        catalog: &Catalog,
        consumer_filter: ConsumerFilter<'a>,
        message_type: &str,
        content_json: serde_json::Value,
    ) -> Result<(), InternalError> {
        match message_type {
            MESSAGE_KAMU_TASK_RUNNING => {
                consume_deserialized_message::<TaskRunningMessage>(
                    catalog,
                    consumer_filter,
                    message_type,
                    content_json,
                )
                .await
            }

            MESSAGE_KAMU_TASK_FINISHED => {
                consume_deserialized_message::<TaskFinishedMessage>(
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
