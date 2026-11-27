// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::Catalog;
use internal_error::InternalError;
use messaging_outbox::{
    InitialConsumerBoundary,
    Message,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const TEST_PRODUCER: &str = "TEST-PRODUCER";
pub(crate) const TEST_CONSUMER: &str = "TestMessageConsumer";

pub(crate) const TEST_PRODUCER_FAILING: &str = "TEST-PRODUCER-FAILING";
const TEST_CONSUMER_FAILING: &str = "FailingTestMessageConsumer";

pub(crate) const TEST_PRODUCER_WITHOUT_CONSUMERS: &str = "TEST-PRODUCER-WITHOUT-CONSUMERS";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TestMessage {
    pub(crate) seq: i64,
}

impl Message for TestMessage {
    fn version() -> u32 {
        1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the sequence numbers of the messages it consumes
pub(crate) struct TestMessageConsumer {
    consumed_seq: Arc<Mutex<Vec<i64>>>,
}

#[dill::component(pub(crate))]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<TestMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: TEST_CONSUMER,
    feeding_producers: &[TEST_PRODUCER],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
impl TestMessageConsumer {
    fn new() -> Self {
        Self {
            consumed_seq: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn consumed_seq(&self) -> Vec<i64> {
        self.consumed_seq.lock().unwrap().clone()
    }
}

impl MessageConsumer for TestMessageConsumer {}

#[async_trait::async_trait]
impl MessageConsumerT<TestMessage> for TestMessageConsumer {
    async fn consume_message(
        &self,
        _target_catalog: &Catalog,
        message: &TestMessage,
    ) -> Result<(), InternalError> {
        self.consumed_seq.lock().unwrap().push(message.seq);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FailingTestMessage {}

impl Message for FailingTestMessage {
    fn version() -> u32 {
        1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FailingTestMessageConsumer {}

#[dill::component(pub(crate))]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<FailingTestMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: TEST_CONSUMER_FAILING,
    feeding_producers: &[TEST_PRODUCER_FAILING],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
impl FailingTestMessageConsumer {
    fn new() -> Self {
        Self {}
    }
}

impl MessageConsumer for FailingTestMessageConsumer {}

#[async_trait::async_trait]
impl MessageConsumerT<FailingTestMessage> for FailingTestMessageConsumer {
    async fn consume_message(
        &self,
        _target_catalog: &Catalog,
        _message: &FailingTestMessage,
    ) -> Result<(), InternalError> {
        Err(InternalError::new("Consumer always fails"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
