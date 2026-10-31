// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use dill::*;
use internal_error::InternalError;
use kamu_datasets::{DatasetExternallyChangedMessage, MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Hears every `DatasetExternallyChangedMessage` the HTTP adapter posts, at
/// once and inside the poster's transaction, and records the head each one
/// announces. When armed it fails the next one, which fails the post
pub struct FailingDatasetChangeListener {
    fail_next: Mutex<bool>,
    announced_heads: Mutex<Vec<odf::Multihash>>,
}

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetExternallyChangedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "FailingDatasetChangeListener",
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER],
    consumption_mode: MessageConsumptionMode::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
#[scope(Singleton)]
impl FailingDatasetChangeListener {
    pub fn new() -> Self {
        Self {
            fail_next: Mutex::new(false),
            announced_heads: Mutex::new(Vec::new()),
        }
    }

    /// Arms the next message to fail
    pub fn fail_next(&self) {
        *self.fail_next.lock().unwrap() = true;
    }

    /// The new head of every message heard, failed ones included, in order
    pub fn announced_heads(&self) -> Vec<odf::Multihash> {
        self.announced_heads.lock().unwrap().clone()
    }
}

impl MessageConsumer for FailingDatasetChangeListener {}

#[async_trait::async_trait]
impl MessageConsumerT<DatasetExternallyChangedMessage> for FailingDatasetChangeListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetExternallyChangedMessage,
    ) -> Result<(), InternalError> {
        let new_head = match message {
            DatasetExternallyChangedMessage::HttpIngest(message) => &message.new_block_hash,
            DatasetExternallyChangedMessage::SmartTransferProtocolSync(message) => {
                &message.new_block_hash
            }
        };
        self.announced_heads.lock().unwrap().push(new_head.clone());

        if std::mem::take(&mut *self.fail_next.lock().unwrap()) {
            return InternalError::bail("The dataset change message failed by the test");
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
