// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository;
use messaging_outbox::{
    Message,
    Outbox,
    OutboxExt,
    OutboxMessageID,
    OutboxMessageRepository,
    OutboxTransactionalImpl,
};
use serde::{Deserialize, Serialize};
use time_source::SystemTimeSourceDefault;

use crate::test_message_type;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_A: &str = "TEST-PRODUCER-A";
const TEST_PRODUCER_B: &str = "TEST-PRODUCER-B";

test_message_type!(A);
test_message_type!(B);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transactional_outbox_messages_of_one_type() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageA {
        body: "bar".to_string(),
    };

    let harness = TransactionalOutboxHarness::new();

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

    let messages: Vec<_> = harness
        .get_saved_messages::<TestMessageA>(TEST_PRODUCER_A)
        .await;
    assert_eq!(messages, vec![message_1, message_2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transactional_outbox_messages_of_two_types() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageB {
        body: "bar".to_string(),
    };

    let harness = TransactionalOutboxHarness::new();

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

    let messages: Vec<_> = harness
        .get_saved_messages::<TestMessageA>(TEST_PRODUCER_A)
        .await;
    assert_eq!(messages, vec![message_1]);

    let messages: Vec<_> = harness
        .get_saved_messages::<TestMessageB>(TEST_PRODUCER_B)
        .await;
    assert_eq!(messages, vec![message_2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TransactionalOutboxHarness {
    _catalog: Catalog,
    outbox: Arc<dyn Outbox>,
    outbox_message_repository: Arc<dyn OutboxMessageRepository>,
}

impl TransactionalOutboxHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<OutboxTransactionalImpl>();
        b.bind::<dyn Outbox, OutboxTransactionalImpl>();
        b.add::<InMemoryOutboxMessageRepository>();
        b.add::<SystemTimeSourceDefault>();

        let catalog = b.build();

        let outbox: Arc<dyn Outbox> = catalog.get_one::<dyn Outbox>().unwrap();
        let outbox_message_repository = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

        Self {
            _catalog: catalog,
            outbox,
            outbox_message_repository,
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
