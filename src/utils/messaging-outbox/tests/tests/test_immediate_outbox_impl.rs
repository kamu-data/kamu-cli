// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::{Arc, Mutex};

use dill::*;
use internal_error::InternalError;
use messaging_outbox::*;
use serde::{Deserialize, Serialize};

use crate::{test_message_consumer, test_message_type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_A: &str = "TEST-PRODUCER-A";
const TEST_PRODUCER_B: &str = "TEST-PRODUCER-B";
const TEST_PRODUCER_C: &str = "TEST-PRODUCER-C";
const TEST_PRODUCER_D: &str = "TEST-PRODUCER-D";

test_message_type!(A);
test_message_type!(B);
test_message_type!(C); // No consumers
test_message_type!(D);

test_message_consumer!(A, A, TEST_PRODUCER_A, BestEffort);
test_message_consumer!(B, B, TEST_PRODUCER_B, BestEffort);
test_message_consumer!(D, D1, TEST_PRODUCER_D, BestEffort);
test_message_consumer!(D, D2, TEST_PRODUCER_D, BestEffort);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_immediate_outbox_messages_of_one_type() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageA {
        body: "bar".to_string(),
    };

    let harness = ImmediateOutboxHarness::new();

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_1.clone())
        .await;
    assert_matches!(res, Ok(_));

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(
        harness.test_message_consumer_a.get_messages(),
        vec![message_1, message_2]
    );
    assert_eq!(harness.test_message_consumer_b.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_d1.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_d2.get_messages(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_immediate_outbox_messages_of_two_types() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageB {
        body: "bar".to_string(),
    };

    let harness = ImmediateOutboxHarness::new();

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_1.clone())
        .await;
    assert_matches!(res, Ok(_));

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_B, message_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(
        harness.test_message_consumer_a.get_messages(),
        vec![message_1]
    );
    assert_eq!(
        harness.test_message_consumer_b.get_messages(),
        vec![message_2]
    );
    assert_eq!(harness.test_message_consumer_d1.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_d2.get_messages(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_immediate_outbox_message_without_consumers() {
    let message = TestMessageC {
        body: "foo".to_string(),
    };

    let harness = ImmediateOutboxHarness::new();

    let res = harness.outbox.post_message(TEST_PRODUCER_C, message).await;
    assert_matches!(res, Ok(_));

    assert_eq!(harness.test_message_consumer_a.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_b.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_d1.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_d2.get_messages(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_immediate_outbox_messages_two_handlers_for_same() {
    let message_1 = TestMessageD {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageD {
        body: "bar".to_string(),
    };

    let harness = ImmediateOutboxHarness::new();

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_D, message_1.clone())
        .await;
    assert_matches!(res, Ok(_));

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_D, message_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(harness.test_message_consumer_a.get_messages(), vec![]);
    assert_eq!(harness.test_message_consumer_b.get_messages(), vec![]);
    assert_eq!(
        harness.test_message_consumer_d1.get_messages(),
        vec![message_1.clone(), message_2.clone()]
    );
    assert_eq!(
        harness.test_message_consumer_d2.get_messages(),
        vec![message_1, message_2]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ImmediateOutboxHarness {
    _catalog: Catalog,
    outbox: Arc<dyn Outbox>,
    test_message_consumer_a: Arc<TestMessageConsumerA>,
    test_message_consumer_b: Arc<TestMessageConsumerB>,
    test_message_consumer_d1: Arc<TestMessageConsumerD1>,
    test_message_consumer_d2: Arc<TestMessageConsumerD2>,
}

impl ImmediateOutboxHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add_builder(
            messaging_outbox::OutboxImmediateImpl::builder()
                .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
        );
        b.bind::<dyn Outbox, OutboxImmediateImpl>();
        b.add::<TestMessageConsumerA>();
        b.add::<TestMessageConsumerB>();
        b.add::<TestMessageConsumerD1>();
        b.add::<TestMessageConsumerD2>();
        register_message_dispatcher::<TestMessageA>(&mut b, TEST_PRODUCER_A);
        register_message_dispatcher::<TestMessageB>(&mut b, TEST_PRODUCER_B);
        register_message_dispatcher::<TestMessageD>(&mut b, TEST_PRODUCER_D);

        let catalog = b.build();

        let outbox = catalog.get_one::<dyn Outbox>().unwrap();
        let test_message_consumer_a = catalog.get_one::<TestMessageConsumerA>().unwrap();
        let test_message_consumer_b = catalog.get_one::<TestMessageConsumerB>().unwrap();
        let test_message_consumer_d1 = catalog.get_one::<TestMessageConsumerD1>().unwrap();
        let test_message_consumer_d2 = catalog.get_one::<TestMessageConsumerD2>().unwrap();

        Self {
            _catalog: catalog,
            outbox,
            test_message_consumer_a,
            test_message_consumer_b,
            test_message_consumer_d1,
            test_message_consumer_d2,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
