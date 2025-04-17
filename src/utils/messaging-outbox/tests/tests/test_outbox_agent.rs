// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use database_common::NoOpDatabasePlugin;
use dill::*;
use init_on_startup::InitOnStartup;
use internal_error::InternalError;
use kamu_messaging_outbox_inmem::{
    InMemoryOutboxMessageConsumptionRepository,
    InMemoryOutboxMessageRepository,
};
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

test_message_consumer!(A, A, TEST_PRODUCER_A, Transactional, All);
test_message_consumer!(A, A1, TEST_PRODUCER_A, Transactional, Latest);
test_message_consumer!(B, B, TEST_PRODUCER_B, Transactional, All);
test_message_consumer!(C, C1, TEST_PRODUCER_C, Transactional, All);
test_message_consumer!(C, C2, TEST_PRODUCER_C, Transactional, All);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_messages_of_one_type() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageA {
        body: "bar".to_string(),
    };

    let harness = OutboxAgentHarness::new();
    harness.outbox_agent.run_initialization().await.unwrap();

    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_1.clone())
        .await
        .unwrap();
    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_2.clone())
        .await
        .unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // Should be delivered now
    harness.check_delivered_messages(&[message_1, message_2], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 2),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 2),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_messages_of_two_types() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageB {
        body: "bar".to_string(),
    };

    let harness = OutboxAgentHarness::new();
    harness.outbox_agent.run_initialization().await.unwrap();

    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_1.clone())
        .await
        .unwrap();
    harness
        .outbox
        .post_message(TEST_PRODUCER_B, message_2.clone())
        .await
        .unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // Should be delivered now
    harness.check_delivered_messages(&[message_1], &[message_2], &[], &[]);

    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 1),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 2),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 1),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 1),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_messages_multiple_consumers() {
    let message_1 = TestMessageC {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageC {
        body: "bar".to_string(),
    };

    let harness = OutboxAgentHarness::new();
    harness.outbox_agent.run_initialization().await.unwrap();

    harness
        .outbox
        .post_message(TEST_PRODUCER_C, message_1.clone())
        .await
        .unwrap();
    harness
        .outbox
        .post_message(TEST_PRODUCER_C, message_2.clone())
        .await
        .unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // Should be delivered now
    harness.check_delivered_messages(
        &[],
        &[],
        &[message_1.clone(), message_2.clone()],
        &[message_1, message_2],
    );

    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 2),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 2),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 2),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 2),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_messages_with_partial_consumption() {
    let harness = OutboxAgentHarness::new();
    harness.outbox_agent.run_initialization().await.unwrap();

    let message_texts = ["foo", "bar", "baz", "super", "duper"];
    for message_text in message_texts {
        harness
            .outbox
            .post_message(
                TEST_PRODUCER_C,
                TestMessageC {
                    body: message_text.to_string(),
                },
            )
            .await
            .unwrap();
    }

    // Let's assume some initial partial boundaries
    harness
        .outbox_consumption_repository
        .update_consumption_boundary(OutboxMessageConsumptionBoundary {
            producer_name: TEST_PRODUCER_C.to_string(),
            consumer_name: "TestMessageConsumerC1".to_string(),
            last_consumed_message_id: OutboxMessageID::new(2),
        })
        .await
        .unwrap();
    harness
        .outbox_consumption_repository
        .update_consumption_boundary(OutboxMessageConsumptionBoundary {
            producer_name: TEST_PRODUCER_C.to_string(),
            consumer_name: "TestMessageConsumerC2".to_string(),
            last_consumed_message_id: OutboxMessageID::new(4),
        })
        .await
        .unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 2),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 4),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    harness.check_delivered_messages(
        &[],
        &[],
        &message_texts[2..]
            .iter()
            .map(|text| TestMessageC {
                body: (*text).to_string(),
            })
            .collect::<Vec<_>>(),
        &message_texts[4..]
            .iter()
            .map(|text| TestMessageC {
                body: (*text).to_string(),
            })
            .collect::<Vec<_>>(),
    );
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 5),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 5),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 5 - 2),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 5 - 4),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_consumer_with_latest_messages() {
    let message_1 = TestMessageA {
        body: "foo".to_string(),
    };
    let message_2 = TestMessageA {
        body: "bar".to_string(),
    };

    let harness = OutboxAgentHarness::new();
    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_1.clone())
        .await
        .unwrap();
    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_2.clone())
        .await
        .unwrap();

    harness.outbox_agent.run_initialization().await.unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 0),
            // For consumers with latest initial_consumer_boundary we set boundary to last produced
            // message id
            (TEST_PRODUCER_A, "TestMessageConsumerA1", 2),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // Should be delivered now
    harness.check_delivered_messages(&[message_1, message_2], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 2),
            (TEST_PRODUCER_A, "TestMessageConsumerA1", 2),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 2 messages
    // Consumer TestMessageConsumerA1 should consume 0 messages
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 2),
        (TEST_PRODUCER_A, "TestMessageConsumerA1", 0),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
    ]);

    let message_3 = TestMessageA {
        body: "baz".to_string(),
    };
    harness
        .outbox
        .post_message(TEST_PRODUCER_A, message_3.clone())
        .await
        .unwrap();

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // harness.check_delivered_messages(&predefined_messages, &[], &[], &[]);

    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, "TestMessageConsumerA", 3),
            (TEST_PRODUCER_A, "TestMessageConsumerA1", 3),
            (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
            (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 3 messages
    // Consumer TestMessageConsumerA1 should consume 1 message
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, "TestMessageConsumerA", 3),
        (TEST_PRODUCER_A, "TestMessageConsumerA1", 1),
        (TEST_PRODUCER_B, "TestMessageConsumerB", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC1", 0),
        (TEST_PRODUCER_C, "TestMessageConsumerC2", 0),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct OutboxAgentHarness {
    catalog: Catalog,
    outbox_agent: Arc<OutboxAgent>,
    outbox: Arc<dyn Outbox>,
    outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>,
    metrics: Arc<OutboxAgentMetrics>,
}

impl OutboxAgentHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<OutboxAgent>();
        b.add::<OutboxAgentMetrics>();
        b.add_value(OutboxConfig::default());
        b.add::<InMemoryOutboxMessageRepository>();
        b.add::<InMemoryOutboxMessageConsumptionRepository>();
        b.add::<OutboxTransactionalImpl>();
        b.bind::<dyn Outbox, OutboxTransactionalImpl>();
        b.add::<SystemTimeSourceDefault>();

        b.add::<TestMessageConsumerA>();
        b.add::<TestMessageConsumerA1>();
        b.add::<TestMessageConsumerB>();
        b.add::<TestMessageConsumerC1>();
        b.add::<TestMessageConsumerC2>();

        register_message_dispatcher::<TestMessageA>(&mut b, TEST_PRODUCER_A);
        register_message_dispatcher::<TestMessageB>(&mut b, TEST_PRODUCER_B);
        register_message_dispatcher::<TestMessageC>(&mut b, TEST_PRODUCER_C);

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let outbox_agent = catalog.get_one::<OutboxAgent>().unwrap();
        let outbox = catalog.get_one::<dyn Outbox>().unwrap();
        let outbox_consumption_repository = catalog
            .get_one::<dyn OutboxMessageConsumptionRepository>()
            .unwrap();
        let metrics = catalog.get_one().unwrap();

        Self {
            catalog,
            outbox_agent,
            outbox,
            outbox_consumption_repository,
            metrics,
        }
    }

    fn check_delivered_messages(
        &self,
        a_messages: &[TestMessageA],
        b_messages: &[TestMessageB],
        c1_messages: &[TestMessageC],
        c2_messages: &[TestMessageC],
    ) {
        let test_message_consumer_a = self.catalog.get_one::<TestMessageConsumerA>().unwrap();
        let test_message_consumer_b = self.catalog.get_one::<TestMessageConsumerB>().unwrap();
        let test_message_consumer_c1 = self.catalog.get_one::<TestMessageConsumerC1>().unwrap();
        let test_message_consumer_c2 = self.catalog.get_one::<TestMessageConsumerC2>().unwrap();

        assert_eq!(test_message_consumer_a.get_messages(), a_messages);
        assert_eq!(test_message_consumer_b.get_messages(), b_messages);
        assert_eq!(test_message_consumer_c1.get_messages(), c1_messages);
        assert_eq!(test_message_consumer_c2.get_messages(), c2_messages);
    }

    async fn check_consumption_boundaries(&self, patterns: &[(&str, &str, i64)]) {
        let boundaries = self.read_consumption_boundaries().await;
        assert_eq!(
            boundaries,
            patterns
                .iter()
                .map(|pattern| OutboxMessageConsumptionBoundary {
                    producer_name: pattern.0.to_string(),
                    consumer_name: pattern.1.to_string(),
                    last_consumed_message_id: OutboxMessageID::new(pattern.2),
                })
                .collect::<Vec<_>>()
        );
    }

    async fn read_consumption_boundaries(&self) -> Vec<OutboxMessageConsumptionBoundary> {
        use futures::TryStreamExt;
        let mut boundaries: Vec<_> = self
            .outbox_consumption_repository
            .list_consumption_boundaries()
            .try_collect()
            .await
            .unwrap();

        boundaries.sort();
        boundaries
    }

    fn check_metrics_messages_processed_total(&self, expected: &[(&str, &str, u64)]) {
        for (producer, consumer, expected_value) in expected {
            let actual_value = self
                .metrics
                .messages_processed_total
                .get_metric_with_label_values(&[producer, consumer])
                .unwrap()
                .get();
            assert_eq!(
                *expected_value, actual_value,
                "messages_processed_total{{producer={producer},consumer={consumer}}}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
