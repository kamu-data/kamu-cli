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
use messaging_outbox::{NewOutboxMessage, OutboxMessage, OutboxMessageID, OutboxMessageRepository};
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
        },
        NewOutboxMessage {
            producer_name: "A".to_string(),
            content_json: serde_json::to_value(&MessageA { x: 27, y: 315 }).unwrap(),
            occurred_on: Utc::now(),
        },
        NewOutboxMessage {
            producer_name: "B".to_string(),
            content_json: serde_json::to_value(&MessageB {
                a: "test".to_string(),
                b: vec!["foo".to_string(), "bar".to_string()],
            })
            .unwrap(),
            occurred_on: Utc::now(),
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
            })
            .await
            .unwrap();
    }

    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
        })
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i * 2);
        assert_eq!(original_message.y, u64::try_from(256 + i).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_producer_messages("A", OutboxMessageID::new(0), 3)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    for i in 1..4 {
        let message = messages.get(i - 1).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_producer_messages("A", OutboxMessageID::new(5), 4)
        .await
        .unwrap()
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
        outbox_message_repo
            .push_message(NewOutboxMessage {
                producer_name: "A".to_string(),
                content_json: serde_json::to_value(&MessageA {
                    x: i,
                    y: u64::try_from(i * 2).unwrap(),
                })
                .unwrap(),
                occurred_on: Utc::now(),
            })
            .await
            .unwrap();
    }

    outbox_message_repo
        .push_message(NewOutboxMessage {
            producer_name: "dummy".to_string(),
            content_json: serde_json::to_value("dummy").unwrap(),
            occurred_on: Utc::now(),
        })
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i);
        assert_eq!(original_message.y, u64::try_from(i * 2).unwrap());
    }

    let messages: Vec<_> = outbox_message_repo
        .get_producer_messages("A", OutboxMessageID::new(5), 3)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(messages.len(), 0);

    let messages: Vec<_> = outbox_message_repo
        .get_producer_messages("A", OutboxMessageID::new(3), 6)
        .await
        .unwrap()
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

#[derive(Debug, Serialize, Deserialize)]
struct MessageA {
    x: i32,
    y: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageB {
    a: String,
    b: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
