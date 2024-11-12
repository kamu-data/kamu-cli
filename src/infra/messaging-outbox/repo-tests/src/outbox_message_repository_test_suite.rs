// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use dill::Catalog;
use futures::TryStreamExt;
use messaging_outbox::{
    Message,
    NewOutboxMessage,
    OutboxMessage,
    OutboxMessageID,
    OutboxMessageRepository,
    OUTBOX_MESSAGE_VERSION,
};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_outbox_messages_initially(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

    let messages_by_producer = outbox_message_repo
        .get_latest_message_ids_by_producer()
        .await
        .unwrap();
    assert_eq!(0, messages_by_producer.len());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_messages_from_several_producers(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();
    let messages = vec![
        NewOutboxMessage {
            producer_name: "A".to_string(),
            content_json: serde_json::to_value(&MessageA { x: 35, y: 256 }).unwrap(),
            occurred_on: Utc::now(),
            version: OUTBOX_MESSAGE_VERSION,
        },
        NewOutboxMessage {
            producer_name: "A".to_string(),
            content_json: serde_json::to_value(&MessageA { x: 27, y: 315 }).unwrap(),
            occurred_on: Utc::now(),
            version: OUTBOX_MESSAGE_VERSION,
        },
        NewOutboxMessage {
            producer_name: "B".to_string(),
            content_json: serde_json::to_value(&MessageB {
                a: "test".to_string(),
                b: vec!["foo".to_string(), "bar".to_string()],
            })
            .unwrap(),
            occurred_on: Utc::now(),
            version: OUTBOX_MESSAGE_VERSION,
        },
    ];

    for message in messages {
        outbox_message_repo.push_message(message).await.unwrap();
    }

    let mut message_ids_by_producer = outbox_message_repo
        .get_latest_message_ids_by_producer()
        .await
        .unwrap();
    message_ids_by_producer.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        message_ids_by_producer,
        vec![
            ("A".to_string(), OutboxMessageID::new(2),),
            ("B".to_string(), OutboxMessageID::new(3),),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_many_messages_and_read_parts(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

    for i in 1..=10 {
        outbox_message_repo
            .push_message(NewOutboxMessage {
                producer_name: "A".to_string(),
                content_json: serde_json::to_value(&MessageA {
                    x: i * 2,
                    y: u64::try_from(256 + i).unwrap(),
                })
                .unwrap(),
                occurred_on: Utc::now(),
                version: OUTBOX_MESSAGE_VERSION,
            })
            .await
            .unwrap();
    }

    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
            version: OUTBOX_MESSAGE_VERSION,
        })
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i * 2);
        assert_eq!(original_message.y, u64::try_from(256 + i).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![("A".to_string(), OutboxMessageID::new(0))], 3)
        .try_collect()
        .await
        .unwrap();

    for i in 1..4 {
        let message = messages.get(i - 1).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![("A".to_string(), OutboxMessageID::new(5))], 4)
        .try_collect()
        .await
        .unwrap();

    for i in 6..10 {
        let message = messages.get(i - 6).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_reading_above_max(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

    for i in 1..=5 {
        let message_body = MessageA {
            x: i,
            y: u64::try_from(i * 2).unwrap(),
        };

        outbox_message_repo
            .push_message(NewOutboxMessage {
                producer_name: "A".to_string(),
                content_json: serde_json::to_value(&message_body).unwrap(),
                occurred_on: Utc::now(),
                version: message_body.version(),
            })
            .await
            .unwrap();
    }

    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
            version: OUTBOX_MESSAGE_VERSION,
        })
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i);
        assert_eq!(original_message.y, u64::try_from(i * 2).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![("A".to_string(), OutboxMessageID::new(5))], 3)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 0);

    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![("A".to_string(), OutboxMessageID::new(3))], 6)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 2); // 4, 5

    for i in 4..=5 {
        let message = messages.get(i - 4).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reading_messages_above_max_with_multiple_producers(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

    // Push a mix of messages for A and B producers (10 A, 5 B)
    for i in 1..=10 {
        let message_body = MessageA {
            x: i * 2,
            y: u64::try_from(256 + i).unwrap(),
        };
        outbox_message_repo
            .push_message(NewOutboxMessage {
                producer_name: "A".to_string(),
                content_json: serde_json::to_value(&message_body).unwrap(),
                occurred_on: Utc::now(),
                version: message_body.version(),
            })
            .await
            .unwrap();
        if i % 2 == 0 {
            let message_body = MessageB {
                a: format!("test_{i}"),
                b: vec![format!("foo_{i}"), format!("bar_{i}")],
            };
            outbox_message_repo
                .push_message(NewOutboxMessage {
                    producer_name: "B".to_string(),
                    content_json: serde_json::to_value(&message_body).unwrap(),
                    occurred_on: Utc::now(),
                    version: message_body.version(),
                })
                .await
                .unwrap();
        }
    }

    fn assert_expected_message_a(message: &OutboxMessage, i: i32) {
        let original_message =
            serde_json::from_value::<MessageA>(message.content_json.clone()).unwrap();

        assert_eq!(original_message.x, i * 2);
        assert_eq!(original_message.y, u64::try_from(256 + i).unwrap());
    }

    fn assert_expected_message_b(message: &OutboxMessage, i: i32) {
        let original_message =
            serde_json::from_value::<MessageB>(message.content_json.clone()).unwrap();

        assert_eq!(original_message.a, format!("test_{i}"));
        assert_eq!(
            original_message.b,
            vec![format!("foo_{i}"), format!("bar_{i}")]
        );
    }

    // [00] #01 A (x=2, y=257)
    // [01] #02 A (x=4, y=258)
    // [02] #03 B (a="test_2", b=["foo_2", "bar_2"])
    // [03] #04 A (x=6, y=259)
    // [04] #05 A (x=8, y=260)
    // [05] #06 B (a="test_4", b=["foo_4", "bar_4"])
    // [06] #07 A (x=10, y=261)
    // [07] #08 A (x=12, y=262)
    // [08] #09 B (a="test_6", b=["foo_6", "bar_6"])
    // [09] #10 A (x=14, y=263)
    // [10] #11 A (x=16, y=264)
    // [11] #12 B (a="test_8", b=["foo_8", "bar_8"])
    // [12] #13 A (x=18, y=265)
    // [13] #14 A (x=20, y=266)
    // [14] #15 B (a="test_10", b=["foo_10", "bar_10"])

    // No filters, reads all messages
    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![], 15)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 15);
    assert_expected_message_a(&messages[3], 3);
    assert_expected_message_a(&messages[10], 8);
    assert_expected_message_b(&messages[8], 6);
    assert_expected_message_b(&messages[14], 10);

    // Multiple filters nearby
    let messages: Vec<_> = outbox_message_repo
        .get_messages(
            vec![
                ("A".to_string(), OutboxMessageID::new(11)),
                ("B".to_string(), OutboxMessageID::new(12)),
            ],
            10,
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 3);
    assert_expected_message_a(&messages[0], 9);
    assert_expected_message_a(&messages[1], 10);
    assert_expected_message_b(&messages[2], 10);

    // Multiple filters long distance, B above window
    let messages: Vec<_> = outbox_message_repo
        .get_messages(
            vec![
                ("A".to_string(), OutboxMessageID::new(2)),
                ("B".to_string(), OutboxMessageID::new(9)),
            ],
            4,
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 4);
    assert_expected_message_a(&messages[0], 3);
    assert_expected_message_a(&messages[1], 4);
    assert_expected_message_a(&messages[2], 5);
    assert_expected_message_a(&messages[3], 6);

    // Multiple filters some distance, but overlap
    let messages: Vec<_> = outbox_message_repo
        .get_messages(
            vec![
                ("A".to_string(), OutboxMessageID::new(7)),
                ("B".to_string(), OutboxMessageID::new(3)),
            ],
            4,
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 4);
    assert_expected_message_b(&messages[0], 4);
    assert_expected_message_a(&messages[1], 6);
    assert_expected_message_b(&messages[2], 6);
    assert_expected_message_a(&messages[3], 7);

    // Multiple filters, partially not existing
    let messages: Vec<_> = outbox_message_repo
        .get_messages(
            vec![
                ("A".to_string(), OutboxMessageID::new(10)),
                ("C".to_string(), OutboxMessageID::new(0)),
            ],
            3,
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 3);
    assert_expected_message_a(&messages[0], 8);
    assert_expected_message_a(&messages[1], 9);
    assert_expected_message_a(&messages[2], 10);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_outbox_messages_version(catalog: &Catalog) {
    let outbox_message_repo = catalog.get_one::<dyn OutboxMessageRepository>().unwrap();

    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
            version: 0,
        })
        .await
        .unwrap();
    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
            version: 0,
        })
        .await
        .unwrap();
    let actual_message_body = MessageA { x: 1, y: 2 };

    let actual_message = NewOutboxMessage {
        producer_name: "A".to_string(),
        content_json: serde_json::to_value(&actual_message_body).unwrap(),
        occurred_on: Utc::now(),
        version: actual_message_body.version(),
    };

    outbox_message_repo
        .push_message(actual_message)
        .await
        .unwrap();

    let messages: Vec<_> = outbox_message_repo
        .get_messages(vec![("A".to_string(), OutboxMessageID::new(0))], 3)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(1, messages.len());
    assert_eq!(actual_message_body.version(), messages[0].version);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageA {
    x: i32,
    y: u64,
}

impl Message for MessageA {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageB {
    a: String,
    b: Vec<String>,
}

impl Message for MessageB {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
