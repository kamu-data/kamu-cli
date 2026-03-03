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
use internal_error::InternalError;
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge;
use messaging_outbox::*;
use serde::{Deserialize, Serialize};
use time_source::SystemTimeSourceDefault;

use crate::{test_message_consumer, test_message_failing_consumer, test_message_type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_A: &str = "TEST-PRODUCER-A";
const TEST_PRODUCER_B: &str = "TEST-PRODUCER-B";
const TEST_PRODUCER_C: &str = "TEST-PRODUCER-C";
const TEST_PRODUCER_D: &str = "TEST-PRODUCER-D";
const TEST_PRODUCER_E: &str = "TEST-PRODUCER-E";

const TEST_CONSUMER_A: &str = "TestMessageConsumerA";
const TEST_CONSUMER_A1: &str = "TestMessageConsumerA1";
const TEST_CONSUMER_B: &str = "TestMessageConsumerB";
const TEST_CONSUMER_C1: &str = "TestMessageConsumerC1";
const TEST_CONSUMER_C2: &str = "TestMessageConsumerC2";
const TEST_CONSUMER_D_OK: &str = "TestMessageConsumerDOk";
const TEST_CONSUMER_D_FAILING: &str = "TestMessageConsumerDFailing";
const TEST_CONSUMER_E_FAILING: &str = "TestMessageConsumerEFailing";

test_message_type!(A);
test_message_type!(B);
test_message_type!(C);
test_message_type!(D);
test_message_type!(E);

test_message_consumer!(A, A, TEST_PRODUCER_A, Transactional, All);
test_message_consumer!(A, A1, TEST_PRODUCER_A, Transactional, Latest);
test_message_consumer!(B, B, TEST_PRODUCER_B, Transactional, All);
test_message_consumer!(C, C1, TEST_PRODUCER_C, Transactional, All);
test_message_consumer!(C, C2, TEST_PRODUCER_C, Transactional, All);
test_message_consumer!(D, DOk, TEST_PRODUCER_D, Transactional, All);
test_message_failing_consumer!(D, DFailing, TEST_PRODUCER_D, Transactional, All);
test_message_failing_consumer!(E, EFailing, TEST_PRODUCER_E, Transactional, All);

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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
    ]);

    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    harness.check_metric_pending(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);
    harness.check_metric_pending(TEST_PRODUCER_A, TEST_CONSUMER_A1, 0);
    harness.check_metric_failed(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);
    harness.check_metric_failed(TEST_PRODUCER_A, TEST_CONSUMER_A1, 0);
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 1),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 1),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 2),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 1),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 1),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 1),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
    ]);

    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    harness.check_metric_pending(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);
    harness.check_metric_pending(TEST_PRODUCER_B, TEST_CONSUMER_B, 0);
    harness.check_metric_failed(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);
    harness.check_metric_failed(TEST_PRODUCER_B, TEST_CONSUMER_B, 0);
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 2),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 2),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 2),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 2),
    ]);

    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    harness.check_metric_pending(TEST_PRODUCER_C, TEST_CONSUMER_C1, 0);
    harness.check_metric_pending(TEST_PRODUCER_C, TEST_CONSUMER_C2, 0);
    harness.check_metric_failed(TEST_PRODUCER_C, TEST_CONSUMER_C1, 0);
    harness.check_metric_failed(TEST_PRODUCER_C, TEST_CONSUMER_C2, 0);
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
        .outbox_message_bridge
        .mark_consumed(
            &harness.catalog,
            TEST_PRODUCER_C,
            TEST_CONSUMER_C1,
            OutboxMessageBoundary {
                message_id: OutboxMessageID::new(2),
                tx_id: 0,
            },
        )
        .await
        .unwrap();

    harness
        .outbox_message_bridge
        .mark_consumed(
            &harness.catalog,
            TEST_PRODUCER_C,
            TEST_CONSUMER_C2,
            OutboxMessageBoundary {
                message_id: OutboxMessageID::new(4),
                tx_id: 0,
            },
        )
        .await
        .unwrap();

    // Posted, but not delivered yet!
    harness.check_delivered_messages(&[], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 2),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 4),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 5),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 5),
        ])
        .await;

    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 5 - 2),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 5 - 4),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            // For consumers with latest initial_consumer_boundary we set boundary to last produced
            // message id
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 2 messages
    // Consumer TestMessageConsumerA1 should consume 0 messages
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 3),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 3),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 3 messages
    // Consumer TestMessageConsumerA1 should consume 1 message
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 3),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 1),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_consumer_with_latest_messages_consume_all_messages() {
    // Test run initialization before message posting and will force consumer with
    // Latest initial_consumer_boundary to consume all messages
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
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 0),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 0),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    // Run iteration
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    // Should be delivered now
    harness.check_delivered_messages(&[message_1.clone(), message_2.clone()], &[], &[], &[]);
    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 2 messages
    // Consumer TestMessageConsumerA1 should consume 2 messages
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 2),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 2),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
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

    harness.check_delivered_messages(&[message_1, message_2, message_3], &[], &[], &[]);

    harness
        .check_consumption_boundaries(&[
            (TEST_PRODUCER_A, TEST_CONSUMER_A, 3),
            (TEST_PRODUCER_A, TEST_CONSUMER_A1, 3),
            (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
            (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
        ])
        .await;

    // Consumer TestMessageConsumerA should consume 3 messages
    // Consumer TestMessageConsumerA1 should consume 3 message
    harness.check_metrics_messages_processed_total(&[
        (TEST_PRODUCER_A, TEST_CONSUMER_A, 3),
        (TEST_PRODUCER_A, TEST_CONSUMER_A1, 3),
        (TEST_PRODUCER_B, TEST_CONSUMER_B, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C1, 0),
        (TEST_PRODUCER_C, TEST_CONSUMER_C2, 0),
    ]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_consumer_isolated_and_gauges_updated() {
    let harness = OutboxAgentFailureHarness::new(20);
    harness.outbox_agent.run_initialization().await.unwrap();

    for body in ["d-1", "d-2", "d-3"] {
        harness
            .outbox
            .post_message(
                TEST_PRODUCER_D,
                TestMessageD {
                    body: body.to_string(),
                },
            )
            .await
            .unwrap();
    }
    for body in ["a-1"] {
        harness
            .outbox
            .post_message(
                TEST_PRODUCER_A,
                TestMessageA {
                    body: body.to_string(),
                },
            )
            .await
            .unwrap();
    }

    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    let d_ok = harness.catalog.get_one::<TestMessageConsumerDOk>().unwrap();
    assert_eq!(d_ok.get_messages().len(), 3);

    let d_fail = harness
        .catalog
        .get_one::<TestMessageConsumerDFailing>()
        .unwrap();
    assert_eq!(d_fail.attempts(), 1);

    let a = harness.catalog.get_one::<TestMessageConsumerA>().unwrap();
    assert_eq!(a.get_messages().len(), 1);

    harness
        .assert_boundary_message_id(TEST_PRODUCER_D, TEST_CONSUMER_D_OK, 3)
        .await;
    harness
        .assert_boundary_message_id(TEST_PRODUCER_D, TEST_CONSUMER_D_FAILING, 0)
        .await;
    harness
        .assert_boundary_message_id(TEST_PRODUCER_A, TEST_CONSUMER_A, 4)
        .await;

    harness.check_metric_processed(TEST_PRODUCER_D, TEST_CONSUMER_D_OK, 3);
    harness.check_metric_processed(TEST_PRODUCER_D, TEST_CONSUMER_D_FAILING, 0);
    harness.check_metric_processed(TEST_PRODUCER_A, TEST_CONSUMER_A, 1);

    harness.check_metric_failed(TEST_PRODUCER_D, TEST_CONSUMER_D_FAILING, 1);
    harness.check_metric_failed(TEST_PRODUCER_D, TEST_CONSUMER_D_OK, 0);
    harness.check_metric_failed(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);

    harness.check_metric_pending(TEST_PRODUCER_D, TEST_CONSUMER_D_OK, 0);
    harness.check_metric_pending(TEST_PRODUCER_D, TEST_CONSUMER_D_FAILING, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_all_consumers_failing_interrupts_and_stops_retries_until_restart() {
    let harness = OutboxAgentFailureHarness::new(20);
    harness.outbox_agent.run_initialization().await.unwrap();

    for body in ["e-1", "e-2"] {
        harness
            .outbox
            .post_message(
                TEST_PRODUCER_E,
                TestMessageE {
                    body: body.to_string(),
                },
            )
            .await
            .unwrap();
    }

    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();
    harness
        .outbox_agent
        .run_single_iteration_only()
        .await
        .unwrap();

    let e_fail = harness
        .catalog
        .get_one::<TestMessageConsumerEFailing>()
        .unwrap();
    assert_eq!(e_fail.attempts(), 1);

    harness
        .assert_boundary_message_id(TEST_PRODUCER_E, TEST_CONSUMER_E_FAILING, 0)
        .await;

    harness.check_metric_processed(TEST_PRODUCER_E, TEST_CONSUMER_E_FAILING, 0);
    harness.check_metric_failed(TEST_PRODUCER_E, TEST_CONSUMER_E_FAILING, 1);
    harness.check_metric_pending(TEST_PRODUCER_E, TEST_CONSUMER_E_FAILING, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_while_has_tasks_drains_backlog_with_small_batch_size() {
    let harness = OutboxAgentFailureHarness::new(2);
    harness.outbox_agent.run_initialization().await.unwrap();

    for i in 1..=5 {
        harness
            .outbox
            .post_message(
                TEST_PRODUCER_A,
                TestMessageA {
                    body: format!("a-{i}"),
                },
            )
            .await
            .unwrap();
    }

    harness.outbox_agent.run_while_has_tasks().await.unwrap();

    let a = harness.catalog.get_one::<TestMessageConsumerA>().unwrap();
    assert_eq!(a.get_messages().len(), 5);

    harness
        .assert_boundary_message_id(TEST_PRODUCER_A, TEST_CONSUMER_A, 5)
        .await;

    harness.check_metric_processed(TEST_PRODUCER_A, TEST_CONSUMER_A, 5);
    harness.check_metric_pending(TEST_PRODUCER_A, TEST_CONSUMER_A, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct BaseOutboxCatalogHarness {
    catalog: Catalog,
}

impl BaseOutboxCatalogHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();

        b.add::<OutboxAgentMetrics>();
        b.add::<InMemoryOutboxMessageBridge>();
        b.add::<OutboxTransactionalImpl>();
        b.bind::<dyn Outbox, OutboxTransactionalImpl>();
        b.add::<SystemTimeSourceDefault>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        Self { catalog: b.build() }
    }

    fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct BaseOutboxAgentHarness {
    catalog: Catalog,
    outbox_agent: Arc<dyn OutboxAgent>,
    outbox: Arc<dyn Outbox>,
    outbox_message_bridge: Arc<dyn OutboxMessageBridge>,
    metrics: Arc<OutboxAgentMetrics>,
}

impl BaseOutboxAgentHarness {
    fn from_catalog(catalog: Catalog) -> Self {
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().unwrap();
        let outbox = catalog.get_one::<dyn Outbox>().unwrap();
        let outbox_message_bridge = catalog.get_one::<dyn OutboxMessageBridge>().unwrap();
        let metrics = catalog.get_one().unwrap();

        Self {
            catalog,
            outbox_agent,
            outbox,
            outbox_message_bridge,
            metrics,
        }
    }

    async fn get_boundary(
        &self,
        producer_name: &str,
        consumer_name: &str,
    ) -> OutboxMessageBoundary {
        let boundaries = self
            .outbox_message_bridge
            .list_consumption_boundaries(&self.catalog)
            .await
            .unwrap();

        boundaries
            .into_iter()
            .find(|b| b.producer_name == producer_name && b.consumer_name == consumer_name)
            .map(|b| b.boundary())
            .unwrap_or_default()
    }

    async fn assert_boundary_message_id(
        &self,
        producer_name: &str,
        consumer_name: &str,
        expected_message_id: i64,
    ) {
        assert_eq!(
            self.get_boundary(producer_name, consumer_name).await,
            OutboxMessageBoundary {
                message_id: OutboxMessageID::new(expected_message_id),
                tx_id: 0,
            }
        );
    }

    fn check_metric_processed(&self, producer: &str, consumer: &str, expected_value: u64) {
        let actual_value = self
            .metrics
            .messages_processed_total
            .get_metric_with_label_values(&[producer, consumer])
            .unwrap()
            .get();
        assert_eq!(expected_value, actual_value);
    }

    fn check_metric_pending(&self, producer: &str, consumer: &str, expected_value: i64) {
        let actual_value = self
            .metrics
            .messages_pending_total
            .get_metric_with_label_values(&[producer, consumer])
            .unwrap()
            .get();
        assert_eq!(expected_value, actual_value);
    }

    fn check_metric_failed(&self, producer: &str, consumer: &str, expected_value: i64) {
        let actual_value = self
            .metrics
            .failed_consumers_total
            .get_metric_with_label_values(&[producer, consumer])
            .unwrap()
            .get();
        assert_eq!(expected_value, actual_value);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseOutboxAgentHarness, base_harness)]
struct OutboxAgentFailureHarness {
    base_harness: BaseOutboxAgentHarness,
}

impl OutboxAgentFailureHarness {
    fn new(batch_size: usize) -> Self {
        let base_catalog_harness = BaseOutboxCatalogHarness::new();

        let mut b = CatalogBuilder::new_chained(base_catalog_harness.catalog());
        b.add::<OutboxAgentImpl>();
        b.add_value(OutboxAgentConfig {
            min_debounce_interval: std::time::Duration::from_millis(1),
            max_listening_timeout: std::time::Duration::from_millis(1),
            batch_size,
        });

        b.add::<TestMessageConsumerA>();
        b.add::<TestMessageConsumerDOk>();
        b.add::<TestMessageConsumerDFailing>();
        b.add::<TestMessageConsumerEFailing>();

        register_message_dispatcher::<TestMessageA>(&mut b, TEST_PRODUCER_A);
        register_message_dispatcher::<TestMessageD>(&mut b, TEST_PRODUCER_D);
        register_message_dispatcher::<TestMessageE>(&mut b, TEST_PRODUCER_E);

        let catalog = b.build();

        Self {
            base_harness: BaseOutboxAgentHarness::from_catalog(catalog),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseOutboxAgentHarness, base_harness)]
struct OutboxAgentHarness {
    base_harness: BaseOutboxAgentHarness,
}

impl OutboxAgentHarness {
    fn new() -> Self {
        let base_catalog_harness = BaseOutboxCatalogHarness::new();

        let mut b = CatalogBuilder::new_chained(base_catalog_harness.catalog());
        b.add::<OutboxAgentImpl>();
        b.add_value(OutboxAgentConfig::local_default());

        b.add::<TestMessageConsumerA>();
        b.add::<TestMessageConsumerA1>();
        b.add::<TestMessageConsumerB>();
        b.add::<TestMessageConsumerC1>();
        b.add::<TestMessageConsumerC2>();

        register_message_dispatcher::<TestMessageA>(&mut b, TEST_PRODUCER_A);
        register_message_dispatcher::<TestMessageB>(&mut b, TEST_PRODUCER_B);
        register_message_dispatcher::<TestMessageC>(&mut b, TEST_PRODUCER_C);

        let catalog = b.build();

        Self {
            base_harness: BaseOutboxAgentHarness::from_catalog(catalog),
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
                    last_tx_id: 0,
                })
                .collect::<Vec<_>>()
        );
    }

    async fn read_consumption_boundaries(&self) -> Vec<OutboxMessageConsumptionBoundary> {
        let mut boundaries: Vec<_> = self
            .outbox_message_bridge
            .list_consumption_boundaries(&self.catalog)
            .await
            .unwrap();

        boundaries.sort();
        boundaries
    }

    fn check_metrics_messages_processed_total(&self, expected: &[(&str, &str, u64)]) {
        for (producer, consumer, expected_value) in expected {
            self.check_metric_processed(producer, consumer, *expected_value);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
