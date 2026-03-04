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
use messaging_outbox::{
    Message,
    NewOutboxMessage,
    OutboxMessage,
    OutboxMessageBoundary,
    OutboxMessageBridge,
    OutboxMessageID,
};
use serde::{Deserialize, Serialize};

const OUTBOX_MESSAGE_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_outbox_messages_initially(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    let messages_boundaries_by_producer = outbox_message_bridge
        .get_latest_message_boundaries_by_producer(catalog)
        .await
        .unwrap();
    assert_eq!(0, messages_boundaries_by_producer.len());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_messages_from_several_producers(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();
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
        outbox_message_bridge
            .push_message(catalog, message)
            .await
            .unwrap();
    }

    let mut message_boundaries_by_producer = outbox_message_bridge
        .get_latest_message_boundaries_by_producer(catalog)
        .await
        .unwrap();
    message_boundaries_by_producer.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(2, message_boundaries_by_producer.len());
    assert_eq!(
        message_boundaries_by_producer[0].0,
        "A".to_string(),
        "Producer A should be present"
    );
    assert_eq!(
        message_boundaries_by_producer[0].1.message_id,
        OutboxMessageID::new(2),
        "Producer A should have correct boundary"
    );

    assert_eq!(
        message_boundaries_by_producer[1].0,
        "B".to_string(),
        "Producer B should be present"
    );
    assert_eq!(
        message_boundaries_by_producer[1].1.message_id,
        OutboxMessageID::new(3),
        "Producer B should have correct boundary"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_many_messages_and_read_parts(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    for i in 1..=10 {
        outbox_message_bridge
            .push_message(
                catalog,
                NewOutboxMessage {
                    producer_name: "A".to_string(),
                    content_json: serde_json::to_value(&MessageA {
                        x: i * 2,
                        y: u64::try_from(256 + i).unwrap(),
                    })
                    .unwrap(),
                    occurred_on: Utc::now(),
                    version: OUTBOX_MESSAGE_VERSION,
                },
            )
            .await
            .unwrap();
    }

    outbox_message_bridge
        .push_message(
            catalog,
            NewOutboxMessage {
                producer_name: "dummy".to_string(),
                content_json: serde_json::to_value("dummy").unwrap(),
                occurred_on: Utc::now(),
                version: OUTBOX_MESSAGE_VERSION,
            },
        )
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i * 2);
        assert_eq!(original_message.y, u64::try_from(256 + i).unwrap());
    }

    let messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", OutboxMessageBoundary::default(), 3)
        .await
        .unwrap();

    for i in 1..4 {
        let message = messages.get(i - 1).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }

    let first_five_messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", OutboxMessageBoundary::default(), 5)
        .await
        .unwrap();
    let boundary_after_5 = boundary_from_message(first_five_messages.get(4).unwrap());

    let messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", boundary_after_5, 4)
        .await
        .unwrap();

    for i in 6..10 {
        let message = messages.get(i - 6).unwrap().clone();
        assert_expected_message_a(message, i32::try_from(i).unwrap());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_reading_above_max(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    for i in 1..=5 {
        outbox_message_bridge
            .push_message(
                catalog,
                NewOutboxMessage {
                    producer_name: "A".to_string(),
                    content_json: serde_json::to_value(&MessageA {
                        x: i,
                        y: u64::try_from(i * 2).unwrap(),
                    })
                    .unwrap(),
                    occurred_on: Utc::now(),
                    version: MessageA::version(),
                },
            )
            .await
            .unwrap();
    }

    outbox_message_bridge
        .push_message(
            catalog,
            NewOutboxMessage {
                producer_name: "dummy".to_string(),
                content_json: serde_json::to_value("dummy").unwrap(),
                occurred_on: Utc::now(),
                version: OUTBOX_MESSAGE_VERSION,
            },
        )
        .await
        .unwrap();

    fn assert_expected_message_a(message: OutboxMessage, i: i32) {
        let original_message = serde_json::from_value::<MessageA>(message.content_json).unwrap();

        assert_eq!(original_message.x, i);
        assert_eq!(original_message.y, u64::try_from(i * 2).unwrap());
    }

    let latest_boundaries = outbox_message_bridge
        .get_latest_message_boundaries_by_producer(catalog)
        .await
        .unwrap();
    let boundary_after_5 = latest_boundaries
        .into_iter()
        .find(|(producer, _)| producer == "A")
        .map(|(_, boundary)| boundary)
        .unwrap();

    let messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", boundary_after_5, 3)
        .await
        .unwrap();
    assert_eq!(messages.len(), 0);

    let first_three_messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", OutboxMessageBoundary::default(), 3)
        .await
        .unwrap();
    let boundary_after_3 = boundary_from_message(first_three_messages.get(2).unwrap());

    let messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", boundary_after_3, 6)
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
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    // Push a mix of messages for A and B producers (10 A, 5 B)
    for i in 1..=10 {
        outbox_message_bridge
            .push_message(
                catalog,
                NewOutboxMessage {
                    producer_name: "A".to_string(),
                    content_json: serde_json::to_value(&MessageA {
                        x: i * 2,
                        y: u64::try_from(256 + i).unwrap(),
                    })
                    .unwrap(),
                    occurred_on: Utc::now(),
                    version: MessageA::version(),
                },
            )
            .await
            .unwrap();
        if i % 2 == 0 {
            outbox_message_bridge
                .push_message(
                    catalog,
                    NewOutboxMessage {
                        producer_name: "B".to_string(),
                        content_json: serde_json::to_value(&MessageB {
                            a: format!("test_{i}"),
                            b: vec![format!("foo_{i}"), format!("bar_{i}")],
                        })
                        .unwrap(),
                        occurred_on: Utc::now(),
                        version: MessageB::version(),
                    },
                )
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

    // Both producers from boundary zero, reads all messages
    let all_messages: Vec<_> = outbox_message_bridge
        .get_unprocessed_messages(
            catalog,
            vec![
                ("A".to_string(), OutboxMessageBoundary::default()),
                ("B".to_string(), OutboxMessageBoundary::default()),
            ],
            15,
        )
        .await
        .unwrap();
    assert_eq!(all_messages.len(), 15);
    assert_expected_message_a(&all_messages[3], 3);
    assert_expected_message_a(&all_messages[10], 8);
    assert_expected_message_b(&all_messages[8], 6);
    assert_expected_message_b(&all_messages[14], 10);

    // Multiple filters nearby
    let boundary_a_after_11 = boundary_from_collected_messages(&all_messages, "A", 11);
    let boundary_b_after_12 = boundary_from_collected_messages(&all_messages, "B", 12);

    let messages: Vec<_> = outbox_message_bridge
        .get_unprocessed_messages(
            catalog,
            vec![
                ("A".to_string(), boundary_a_after_11),
                ("B".to_string(), boundary_b_after_12),
            ],
            10,
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 3);
    assert_expected_message_a(&messages[0], 9);
    assert_expected_message_a(&messages[1], 10);
    assert_expected_message_b(&messages[2], 10);

    // Multiple filters long distance, B above window
    let boundary_a_after_2 = boundary_from_collected_messages(&all_messages, "A", 2);
    let boundary_b_after_9 = boundary_from_collected_messages(&all_messages, "B", 9);

    let messages: Vec<_> = outbox_message_bridge
        .get_unprocessed_messages(
            catalog,
            vec![
                ("A".to_string(), boundary_a_after_2),
                ("B".to_string(), boundary_b_after_9),
            ],
            4,
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 4);
    assert_expected_message_a(&messages[0], 3);
    assert_expected_message_a(&messages[1], 4);
    assert_expected_message_a(&messages[2], 5);
    assert_expected_message_a(&messages[3], 6);

    // Multiple filters some distance, but overlap
    let boundary_a_after_7 = boundary_from_collected_messages(&all_messages, "A", 7);
    let boundary_b_after_3 = boundary_from_collected_messages(&all_messages, "B", 3);

    let messages: Vec<_> = outbox_message_bridge
        .get_unprocessed_messages(
            catalog,
            vec![
                ("A".to_string(), boundary_a_after_7),
                ("B".to_string(), boundary_b_after_3),
            ],
            4,
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 4);
    assert_expected_message_b(&messages[0], 4);
    assert_expected_message_a(&messages[1], 6);
    assert_expected_message_b(&messages[2], 6);
    assert_expected_message_a(&messages[3], 7);

    // Multiple filters, partially not existing
    let boundary_a_after_10 = boundary_from_collected_messages(&all_messages, "A", 10);

    let messages: Vec<_> = outbox_message_bridge
        .get_unprocessed_messages(
            catalog,
            vec![
                ("A".to_string(), boundary_a_after_10),
                ("C".to_string(), OutboxMessageBoundary::default()),
            ],
            3,
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 3);
    assert_expected_message_a(&messages[0], 8);
    assert_expected_message_a(&messages[1], 9);
    assert_expected_message_a(&messages[2], 10);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_outbox_messages_version(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    outbox_message_bridge
        .push_message(
            catalog,
            NewOutboxMessage {
                producer_name: "dummy".to_string(),
                content_json: serde_json::to_value("dummy").unwrap(),
                occurred_on: Utc::now(),
                version: 0,
            },
        )
        .await
        .unwrap();
    outbox_message_bridge
        .push_message(
            catalog,
            NewOutboxMessage {
                producer_name: "dummy".to_string(),
                content_json: serde_json::to_value("dummy").unwrap(),
                occurred_on: Utc::now(),
                version: 0,
            },
        )
        .await
        .unwrap();
    let actual_message_body = MessageA { x: 1, y: 2 };

    let actual_message = NewOutboxMessage {
        producer_name: "A".to_string(),
        content_json: serde_json::to_value(&actual_message_body).unwrap(),
        occurred_on: Utc::now(),
        version: MessageA::version(),
    };

    outbox_message_bridge
        .push_message(catalog, actual_message)
        .await
        .unwrap();

    let messages: Vec<_> = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", OutboxMessageBoundary::default(), 3)
        .await
        .unwrap();

    assert_eq!(1, messages.len());
    assert_eq!(MessageA::version(), messages[0].version);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_consumption_boundaries_monotonic_and_isolated(catalog: &Catalog) {
    let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    for i in 1..=3 {
        outbox_message_bridge
            .push_message(
                catalog,
                NewOutboxMessage {
                    producer_name: "A".to_string(),
                    content_json: serde_json::to_value(&MessageA {
                        x: i,
                        y: u64::try_from(i * 10).unwrap(),
                    })
                    .unwrap(),
                    occurred_on: Utc::now(),
                    version: MessageA::version(),
                },
            )
            .await
            .unwrap();
    }

    outbox_message_bridge
        .push_message(
            catalog,
            NewOutboxMessage {
                producer_name: "B".to_string(),
                content_json: serde_json::to_value(&MessageB {
                    a: "only-one".to_string(),
                    b: vec!["x".to_string()],
                })
                .unwrap(),
                occurred_on: Utc::now(),
                version: MessageB::version(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        outbox_message_bridge
            .list_consumption_boundaries(catalog)
            .await
            .unwrap()
            .len(),
        0
    );

    let producer_a_messages = outbox_message_bridge
        .get_messages_by_producer(catalog, "A", OutboxMessageBoundary::default(), 3)
        .await
        .unwrap();
    let producer_b_messages = outbox_message_bridge
        .get_messages_by_producer(catalog, "B", OutboxMessageBoundary::default(), 1)
        .await
        .unwrap();

    let a_boundary_1 = boundary_from_message(producer_a_messages.first().unwrap());
    let a_boundary_2 = boundary_from_message(producer_a_messages.get(1).unwrap());
    let a_boundary_3 = boundary_from_message(producer_a_messages.get(2).unwrap());
    let b_boundary_1 = boundary_from_message(producer_b_messages.first().unwrap());

    outbox_message_bridge
        .mark_consumed(catalog, "A", "consumer-1", a_boundary_2)
        .await
        .unwrap();

    let boundaries = outbox_message_bridge
        .list_consumption_boundaries(catalog)
        .await
        .unwrap();
    assert_eq!(boundaries.len(), 1);
    assert_eq!(
        find_consumption_boundary(&boundaries, "A", "consumer-1").boundary(),
        a_boundary_2
    );

    outbox_message_bridge
        .mark_consumed(catalog, "A", "consumer-1", a_boundary_1)
        .await
        .unwrap();

    let boundaries = outbox_message_bridge
        .list_consumption_boundaries(catalog)
        .await
        .unwrap();
    assert_eq!(
        find_consumption_boundary(&boundaries, "A", "consumer-1").boundary(),
        a_boundary_2
    );

    outbox_message_bridge
        .mark_consumed(catalog, "A", "consumer-1", a_boundary_3)
        .await
        .unwrap();
    outbox_message_bridge
        .mark_consumed(catalog, "A", "consumer-2", a_boundary_1)
        .await
        .unwrap();
    outbox_message_bridge
        .mark_consumed(catalog, "B", "consumer-1", b_boundary_1)
        .await
        .unwrap();

    let boundaries = outbox_message_bridge
        .list_consumption_boundaries(catalog)
        .await
        .unwrap();

    assert_eq!(boundaries.len(), 3);
    assert_eq!(
        find_consumption_boundary(&boundaries, "A", "consumer-1").boundary(),
        a_boundary_3
    );
    assert_eq!(
        find_consumption_boundary(&boundaries, "A", "consumer-2").boundary(),
        a_boundary_1
    );
    assert_eq!(
        find_consumption_boundary(&boundaries, "B", "consumer-1").boundary(),
        b_boundary_1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageA {
    x: i32,
    y: u64,
}

impl Message for MessageA {
    fn version() -> u32 {
        OUTBOX_MESSAGE_VERSION
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageB {
    a: String,
    b: Vec<String>,
}

impl Message for MessageB {
    fn version() -> u32 {
        OUTBOX_MESSAGE_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn boundary_from_collected_messages(
    messages: &[OutboxMessage],
    producer_name: &str,
    message_id: i64,
) -> OutboxMessageBoundary {
    let boundary_message = messages
        .iter()
        .find(|message| {
            message.producer_name == producer_name
                && message.message_id == OutboxMessageID::new(message_id)
        })
        .unwrap_or_else(|| {
            panic!("Missing message boundary for producer={producer_name}, message_id={message_id}")
        });

    boundary_from_message(boundary_message)
}

fn boundary_from_message(message: &OutboxMessage) -> OutboxMessageBoundary {
    OutboxMessageBoundary {
        message_id: message.message_id,
        tx_id: message.tx_id,
    }
}

fn find_consumption_boundary<'a>(
    boundaries: &'a [messaging_outbox::OutboxMessageConsumptionBoundary],
    producer_name: &str,
    consumer_name: &str,
) -> &'a messaging_outbox::OutboxMessageConsumptionBoundary {
    boundaries
        .iter()
        .find(|boundary| {
            boundary.producer_name == producer_name && boundary.consumer_name == consumer_name
        })
        .unwrap_or_else(|| {
            panic!(
                "Missing consumption boundary for producer={producer_name}, \
                 consumer={consumer_name}"
            )
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
