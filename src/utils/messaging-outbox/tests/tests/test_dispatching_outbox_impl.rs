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
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository;
use messaging_outbox::*;
use serde::{Deserialize, Serialize};
use time_source::SystemTimeSourceDefault;

use crate::{test_message_consumer, test_message_type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_A: &str = "TEST-PRODUCER-A";
const TEST_PRODUCER_B: &str = "TEST-PRODUCER-B";
const TEST_PRODUCER_C: &str = "TEST-PRODUCER-C";

test_message_type!(A);
test_message_type!(B);
test_message_type!(C);

test_message_consumer!(A, A, TEST_PRODUCER_A, Immediate, All);
test_message_consumer!(B, B, TEST_PRODUCER_B, Transactional, All);
test_message_consumer!(C, CB, TEST_PRODUCER_C, Immediate, All);
test_message_consumer!(C, CD, TEST_PRODUCER_C, Transactional, All);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_immediate_only_messages() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageA {
        body: "bar".to_string(),
    };

    let harness = DispatchingOutboxHarness::new();

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
        harness.test_consumer_a.get_messages(),
        vec![message_1, message_2]
    );
    assert_eq!(harness.test_consumer_b.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_cb.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_cd.get_messages(), vec![]);

    assert_eq!(
        harness
            .get_saved_messages::<TestMessageA>(TEST_PRODUCER_A)
            .await
            .len(),
        0
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageB>(TEST_PRODUCER_B)
            .await
            .len(),
        0
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageC>(TEST_PRODUCER_C)
            .await
            .len(),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transactional_only_messages() {
    let message_1 = TestMessageB {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageB {
        body: "bar".to_string(),
    };

    let harness = DispatchingOutboxHarness::new();

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_B, message_1.clone())
        .await;
    assert_matches!(res, Ok(_));

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_B, message_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(harness.test_consumer_a.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_b.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_cb.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_cd.get_messages(), vec![]);

    assert_eq!(
        harness
            .get_saved_messages::<TestMessageA>(TEST_PRODUCER_A)
            .await
            .len(),
        0
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageB>(TEST_PRODUCER_B)
            .await,
        vec![message_1, message_2]
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageC>(TEST_PRODUCER_C)
            .await
            .len(),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_messages_mixed_delivery() {
    let message_1 = TestMessageC {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageC {
        body: "bar".to_string(),
    };

    let harness = DispatchingOutboxHarness::new();

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_C, message_1.clone())
        .await;
    assert_matches!(res, Ok(_));

    let res = harness
        .outbox
        .post_message(TEST_PRODUCER_C, message_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(harness.test_consumer_a.get_messages(), vec![]);
    assert_eq!(harness.test_consumer_b.get_messages(), vec![]);
    assert_eq!(
        harness.test_consumer_cb.get_messages(),
        vec![message_1.clone(), message_2.clone()]
    );
    assert_eq!(harness.test_consumer_cd.get_messages(), vec![]);

    assert_eq!(
        harness
            .get_saved_messages::<TestMessageA>(TEST_PRODUCER_A)
            .await
            .len(),
        0
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageB>(TEST_PRODUCER_B)
            .await
            .len(),
        0
    );
    assert_eq!(
        harness
            .get_saved_messages::<TestMessageC>(TEST_PRODUCER_C)
            .await,
        vec![message_1, message_2]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DispatchingOutboxHarness {
    _catalog: Catalog,
    outbox: Arc<dyn Outbox>,
    outbox_message_repository: Arc<dyn OutboxMessageRepository>,
    test_consumer_a: Arc<TestMessageConsumerA>,
    test_consumer_b: Arc<TestMessageConsumerB>,
    test_consumer_cb: Arc<TestMessageConsumerCB>,
    test_consumer_cd: Arc<TestMessageConsumerCD>,
}

impl DispatchingOutboxHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add_builder(messaging_outbox::OutboxImmediateImpl::builder(
            messaging_outbox::ConsumerFilter::ImmediateConsumers,
        ));
        b.add::<OutboxTransactionalImpl>();
        b.add::<OutboxDispatchingImpl>();
        b.bind::<dyn Outbox, OutboxDispatchingImpl>();
        b.add::<InMemoryOutboxMessageRepository>();
        b.add::<SystemTimeSourceDefault>();
        b.add::<TestMessageConsumerA>();
        b.add::<TestMessageConsumerB>();
        b.add::<TestMessageConsumerCB>();
        b.add::<TestMessageConsumerCD>();

        register_message_dispatcher::<TestMessageA>(&mut b, TEST_PRODUCER_A);
        register_message_dispatcher::<TestMessageB>(&mut b, TEST_PRODUCER_B);
        register_message_dispatcher::<TestMessageC>(&mut b, TEST_PRODUCER_C);

        let catalog = b.build();

        let outbox = catalog.get_one::<dyn Outbox>().unwrap();
        let outbox_message_repository = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();
        let test_consumer_a = catalog.get_one::<TestMessageConsumerA>().unwrap();
        let test_consumer_b = catalog.get_one::<TestMessageConsumerB>().unwrap();
        let test_consumer_cb = catalog.get_one::<TestMessageConsumerCB>().unwrap();
        let test_consumer_cd = catalog.get_one::<TestMessageConsumerCD>().unwrap();

        Self {
            _catalog: catalog,
            outbox,
            outbox_message_repository,
            test_consumer_a,
            test_consumer_b,
            test_consumer_cb,
            test_consumer_cd,
        }
    }

    async fn get_saved_messages<TMessage: Message>(&self, producer_name: &str) -> Vec<TMessage> {
        use futures::TryStreamExt;
        let outbox_messages: Vec<_> = self
            .outbox_message_repository
            .get_messages(
                vec![(producer_name.to_owned(), OutboxMessageID::new(0))],
                10,
            )
            .try_collect()
            .await
            .unwrap();

        let decoded_messages: Vec<_> = outbox_messages
            .into_iter()
            .map(|om| {
                assert_eq!(om.producer_name, producer_name);
                serde_json::from_value::<TMessage>(om.content_json).unwrap()
            })
            .collect();

        decoded_messages
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
