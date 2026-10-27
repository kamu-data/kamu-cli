// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_adapter_flow_dataset::{FlowConfigRuleIngest, ingest_dataset_binding};
use kamu_adapter_flow_webhook::{
    DatasetRefUpdatedWatermark,
    FLOW_TYPE_WEBHOOK_DELIVER,
    webhook_deliver_binding,
};
use kamu_adapter_task_dataset::{LogicalPlanDatasetUpdate, TaskResultDatasetUpdate};
use kamu_adapter_task_webhook::{LogicalPlanWebhookDeliver, TaskErrorWebhookDelivery};
use kamu_core::PullResult;
use kamu_datasets::{
    DatasetExternallyChangedMessage,
    GetIncrementError,
    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    MockDatasetIncrementQueryService,
};
use kamu_datasets_services::testing::FakeDatasetReferenceService;
use kamu_flow_system::*;
use kamu_task_system::*;
use kamu_webhooks::{
    WebhookEventTypeCatalog,
    WebhookSendConnectionTimeoutError,
    WebhookSendFailedToConnectError,
    WebhookSubscriptionID,
    WebhookUnsuccessfulResponseError,
    WebhookWatermarkRepository,
};
use messaging_outbox::OutboxExt;
use odf::dataset::MetadataChainIncrementInterval;
use time_source::SystemTimeSource;

use crate::tests::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowActivationArgs,
    ManualFlowActivationDriver,
    TaskDriver,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_delivered_after_dataset_ingested_over_http() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));

    harness
        .simulate_flow_scenario(|| async {
            let push_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: delivery of the push, start running at 30ms, finish at 40ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(60));

            tokio::join!(push_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.webhook_trigger_status().await,
        Some(FlowTriggerStatus::Active)
    );
    assert_eq!(
        harness.completed_flows(FLOW_TYPE_WEBHOOK_DELIVER, "success"),
        1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_delivered_after_upstream_ingest_flow_succeeded() {
    let harness = WebhookFlowHarness::new(3).await;
    harness.configure_ingest().await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));

    harness
        .simulate_flow_scenario(|| async {
            let trigger_handle = harness
                .manual_ingest_driver(Duration::milliseconds(10))
                .run();

            // Task 0: ingest of "foo", start running at 20ms, update the head at 30ms
            let task0_handle =
                harness.run_ingest_task(0, 0, Duration::milliseconds(20), &head_0, &head_1);

            // Task 1: delivery of that update, start running at 40ms, finish at 50ms
            let task1_handle = harness
                .delivery_task_driver(
                    1,
                    1,
                    Duration::milliseconds(40),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(70));

            tokio::join!(trigger_handle, task0_handle, task1_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +20ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting Input(foo) Batching(3/0, until=30ms)

            #6: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting Input(foo) Batching(3/0, until=30ms) Activating(at=30ms)

            #7: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting Input(foo) Executor(task=1, since=30ms)

            #8: +40ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Running(task=1)

            #9: +50ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Finished Success

            "#
        ),
        harness.flows_report()
    );

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_pushes_during_delivery_batched_into_next_flow() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1, head_2, head_3) = (
        WebhookFlowHarness::head(0),
        WebhookFlowHarness::head(1),
        WebhookFlowHarness::head(2),
        WebhookFlowHarness::head(3),
    );

    harness
        .simulate_flow_scenario(|| async {
            let push1_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);
            // Two more pushes while the first delivery runs
            let push2_handle = harness.push_over_http(Duration::milliseconds(40), &head_1, &head_2);
            let push3_handle = harness.push_over_http(Duration::milliseconds(50), &head_2, &head_3);

            // Task 0: delivery of the 1st push, start running at 30ms, finish at 70ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(40),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            // Task 1: one delivery of the 2nd and 3rd pushes, throttled after the 1st
            // delivery, start running at 90ms, finish at 100ms
            let task1_handle = harness
                .delivery_task_driver(
                    1,
                    1,
                    Duration::milliseconds(90),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_1, &head_3),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(120));

            tokio::join!(
                push1_handle,
                push2_handle,
                push3_handle,
                task0_handle,
                task1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +70ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            #6: +70ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting HttpIngest Throttling(for=20ms, wakeup=90ms, shifted=40ms)
                Flow ID = 0 Finished Success

            #7: +90ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting HttpIngest Executor(task=1, since=90ms)
                Flow ID = 0 Finished Success

            #8: +90ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #9: +100ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.completed_flows(FLOW_TYPE_WEBHOOK_DELIVER, "success"),
        2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_delivery_failure_not_retried_without_retry_policy() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));
    let delivery_error = WebhookFlowHarness::unsuccessful_response(500);

    harness
        .simulate_flow_scenario(|| async {
            let push_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: start running at 30ms, receiver responds with 500 at 40ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Failed(delivery_error.clone().into_task_error()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(1100));

            tokio::join!(push_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Failed

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.flow_state(FlowID::new(0)).await.outcome,
        Some(FlowOutcome::Failed(delivery_error.into_task_error()))
    );

    // One failure is below the stop policy threshold
    assert_eq!(
        harness.webhook_trigger_status().await,
        Some(FlowTriggerStatus::Active)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_delivery_retried_by_default_retry_policy() {
    let harness = WebhookFlowHarness::with_default_retry_policy(RetryPolicy {
        max_attempts: 1,
        min_delay_seconds: 1,
        backoff_type: RetryBackoffType::Fixed,
    })
    .await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));

    harness
        .simulate_flow_scenario(|| async {
            let push_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: start running at 30ms, receiver times out at 40ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Failed(
                            WebhookFlowHarness::connection_timeout().into_task_error(),
                        ),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            // Task 1: retry of the same delivery, start running at 1040ms, finish at 1050ms
            let task1_handle = harness
                .delivery_task_driver(
                    1,
                    0,
                    Duration::milliseconds(1040),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(1100));

            tokio::join!(push_handle, task0_handle, task1_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Retrying(scheduled_at=1040ms)

            #6: +1040ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Retrying(scheduled_at=1040ms) Executor(task=1, since=1040ms)

            #7: +1040ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0,1)

            #8: +1050ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.webhook_trigger_status().await,
        Some(FlowTriggerStatus::Active)
    );
    assert_eq!(
        harness.completed_flows_retried_at_most(FLOW_TYPE_WEBHOOK_DELIVER, "success", 1),
        1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_trigger_stopped_after_consecutive_failures() {
    let harness = WebhookFlowHarness::new(2).await;
    let (head_0, head_1, head_2, head_3) = (
        WebhookFlowHarness::head(0),
        WebhookFlowHarness::head(1),
        WebhookFlowHarness::head(2),
        WebhookFlowHarness::head(3),
    );
    let failed_to_connect =
        || TaskOutcome::Failed(WebhookFlowHarness::failed_to_connect().into_task_error());

    harness
        .simulate_flow_scenario(|| async {
            let push1_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);
            let push2_handle = harness.push_over_http(Duration::milliseconds(60), &head_1, &head_2);
            // Arrives after the trigger has stopped: no delivery
            let push3_handle =
                harness.push_over_http(Duration::milliseconds(100), &head_2, &head_3);

            // Task 0: start running at 30ms, fail to connect at 40ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (Duration::milliseconds(10), failed_to_connect()),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            // Task 1: start running at 70ms, fail to connect at 80ms. The receiver
            // never got 0 -> 1, so this delivery covers it too
            let task1_handle = harness
                .delivery_task_driver(
                    1,
                    1,
                    Duration::milliseconds(70),
                    (Duration::milliseconds(10), failed_to_connect()),
                    harness.deliver_plan(&head_0, &head_2),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(150));

            tokio::join!(
                push1_handle,
                push2_handle,
                push3_handle,
                task0_handle,
                task1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Failed

            #6: +60ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting HttpIngest Batching(3/0, until=60ms)
                Flow ID = 0 Finished Failed

            #7: +60ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting HttpIngest Batching(3/0, until=60ms) Activating(at=60ms)
                Flow ID = 0 Finished Failed

            #8: +60ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting HttpIngest Executor(task=1, since=60ms)
                Flow ID = 0 Finished Failed

            #9: +70ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Failed

            #10: +80ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Failed

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.webhook_trigger_status().await,
        Some(FlowTriggerStatus::StoppedAutomatically)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_trigger_stopped_at_once_on_unrecoverable_failure() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));

    harness
        .simulate_flow_scenario(|| async {
            let push_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: start running at 30ms, fail at 40ms. Delivery errors are all
            // recoverable, so an unrecoverable one stands for an internal failure
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(60));

            tokio::join!(push_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Failed

            "#
        ),
        harness.flows_report()
    );

    assert_eq!(
        harness.webhook_trigger_status().await,
        Some(FlowTriggerStatus::StoppedAutomatically)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_delivered_on_breaking_change() {
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));
    let harness = WebhookFlowHarness::with_breaking_change(&head_0, &head_1).await;

    harness
        .simulate_flow_scenario(|| async {
            let push_handle = harness.force_push(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: start running at 30ms, finish at 40ms
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_breaking_change_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(60));

            tokio::join!(push_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting SmartProtocolPush Batching(0/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting SmartProtocolPush Batching(0/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting SmartProtocolPush Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_catches_up_on_update_made_before_agent_started() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1) = (WebhookFlowHarness::head(0), WebhookFlowHarness::head(1));

    // Changed while the agent was down, so no update message reaches the sensor
    harness.set_foo_head(&head_1);

    harness
        .simulate_flow_scenario(|| async {
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(10),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(40));

            tokio::join!(task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting ExternallyDetectedChange Batching(3/0, until=0ms) Activating(at=0ms)

            #1: +0ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting ExternallyDetectedChange Executor(task=0, since=0ms)

            #2: +10ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );
    assert_eq!(
        harness.completed_flows(FLOW_TYPE_WEBHOOK_DELIVER, "success"),
        1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_update_made_while_paused_delivered_on_resume() {
    let harness = WebhookFlowHarness::new(3).await;
    let (head_0, head_1, head_2) = (
        WebhookFlowHarness::head(0),
        WebhookFlowHarness::head(1),
        WebhookFlowHarness::head(2),
    );

    harness
        .simulate_flow_scenario(|| async {
            let push1_handle = harness.push_over_http(Duration::milliseconds(20), &head_0, &head_1);

            // Task 0: delivery of the first push
            let task0_handle = harness
                .delivery_task_driver(
                    0,
                    0,
                    Duration::milliseconds(30),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_0, &head_1),
                )
                .run();

            // The second push lands while the subscription is paused
            let pause_handle = harness.pause_webhook(Duration::milliseconds(50));
            let push2_handle = harness.push_over_http(Duration::milliseconds(60), &head_1, &head_2);
            let resume_handle = harness.resume_webhook(Duration::milliseconds(80));

            // Task 1: delivery of the second push, caught up on resume
            let task1_handle = harness
                .delivery_task_driver(
                    1,
                    1,
                    Duration::milliseconds(90),
                    (
                        Duration::milliseconds(10),
                        TaskOutcome::Success(TaskResult::empty()),
                    ),
                    harness.deliver_plan(&head_1, &head_2),
                )
                .run();

            let main_handle = harness.advance_time(Duration::milliseconds(120));

            tokio::join!(
                push1_handle,
                task0_handle,
                pause_handle,
                push2_handle,
                resume_handle,
                task1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms)

            #2: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Batching(3/0, until=20ms) Activating(at=20ms)

            #3: +20ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Waiting HttpIngest Executor(task=0, since=20ms)

            #4: +30ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Running(task=0)

            #5: +40ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 0 Finished Success

            #6: +80ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting ExternallyDetectedChange Batching(3/0, until=80ms)
                Flow ID = 0 Finished Success

            #7: +80ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting ExternallyDetectedChange Batching(3/0, until=80ms) Activating(at=80ms)
                Flow ID = 0 Finished Success

            #8: +80ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #9: +90ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #10: +100ms:
              "foo" Subscription: 00000000-0000-0000-0000-000000000001 WebhookDeliver:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        ),
        harness.flows_report()
    );
    assert_eq!(
        harness.completed_flows(FLOW_TYPE_WEBHOOK_DELIVER, "success"),
        2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A "foo" root dataset with one dataset-updates webhook subscription, whose
/// trigger is set as the webhooks event bridge sets it
#[oop::extend(FlowHarness, base)]
struct WebhookFlowHarness {
    base: FlowHarness,
    foo_id: odf::DatasetID,
    subscription_id: WebhookSubscriptionID,
    webhook_binding: FlowBinding,
}

impl WebhookFlowHarness {
    async fn new(max_consecutive_failures: u32) -> Self {
        Self::with_overrides(
            FlowHarnessOverrides {
                mock_dataset_changes: Some(Self::new_data_increment()),
                ..Default::default()
            },
            max_consecutive_failures,
        )
        .await
    }

    async fn with_default_retry_policy(retry_policy: RetryPolicy) -> Self {
        Self::with_overrides(
            FlowHarnessOverrides {
                mock_dataset_changes: Some(Self::new_data_increment()),
                default_retry_policy_by_flow_type: HashMap::from([(
                    FLOW_TYPE_WEBHOOK_DELIVER.to_string(),
                    retry_policy,
                )]),
                ..Default::default()
            },
            3,
        )
        .await
    }

    /// The increment between the given heads is invalid, as after a force push
    async fn with_breaking_change(old_head: &odf::Multihash, new_head: &odf::Multihash) -> Self {
        // A rewritten history: neither head is in the history of the other
        let mut dataset_changes =
            MockDatasetIncrementQueryService::with_increment_error_invalid_interval(
                Self::foo_alias_id(),
                old_head.clone(),
                new_head.clone(),
            );
        let (old_head, new_head) = (old_head.clone(), new_head.clone());
        dataset_changes
            .expect_get_increment_between()
            .withf(move |_, old, new| *old == Some(&new_head) && *new == old_head)
            .returning(|_, old, new| {
                Err(GetIncrementError::InvalidInterval(
                    odf::dataset::InvalidIntervalError {
                        head: new.clone(),
                        tail: old.unwrap().clone(),
                    },
                ))
            });

        Self::with_overrides(
            FlowHarnessOverrides {
                mock_dataset_changes: Some(dataset_changes),
                ..Default::default()
            },
            3,
        )
        .await
    }

    async fn with_overrides(
        overrides: FlowHarnessOverrides,
        max_consecutive_failures: u32,
    ) -> Self {
        let base = FlowHarness::with_overrides(overrides);

        let foo_id = base
            .create_root_dataset(odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo"),
                account_name: None,
            })
            .await;
        assert_eq!(foo_id, Self::foo_alias_id());
        Self::dataset_references(&base).set_head(&foo_id, &Self::head(0));

        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::from_u128(1));
        Self::start_watermark(&base, subscription_id).await;
        let webhook_binding = webhook_deliver_binding(
            subscription_id,
            &WebhookEventTypeCatalog::dataset_ref_updated(),
            Some(&foo_id),
        );
        base.set_flow_trigger(
            base.now(),
            webhook_binding.clone(),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::immediate(),
                BreakingChangeRule::Recover,
            )),
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(max_consecutive_failures)
                    .unwrap(),
            },
        )
        .await;

        base.catalog
            .get_one::<FlowSystemTestListener>()
            .unwrap()
            .define_dataset_display_name(foo_id.clone(), "foo".to_string());

        Self {
            base,
            foo_id,
            subscription_id,
            webhook_binding,
        }
    }

    /// The receiver of the subscription starts from `head(0)`
    async fn start_watermark(base: &FlowHarness, subscription_id: WebhookSubscriptionID) {
        let watermark_repo = base
            .catalog
            .get_one::<dyn WebhookWatermarkRepository>()
            .unwrap();
        watermark_repo
            .set_watermark(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
                DatasetRefUpdatedWatermark {
                    head: Self::head(0),
                }
                .into(),
            )
            .await
            .unwrap();
    }

    fn dataset_references(base: &FlowHarness) -> Arc<FakeDatasetReferenceService> {
        base.catalog
            .get_one::<FakeDatasetReferenceService>()
            .unwrap()
    }

    /// Moves the head of "foo" without announcing it, as a change made while
    /// nothing listens
    fn set_foo_head(&self, head: &odf::Multihash) {
        Self::dataset_references(&self.base).set_head(&self.foo_id, head);
    }

    /// After `run_since_start` of virtual time, pauses the subscription's trigger
    async fn pause_webhook(&self, run_since_start: Duration) {
        self.fake_system_time_source.sleep(run_since_start).await;
        self.pause_flow(self.now(), &self.webhook_binding).await;
    }

    /// After `run_since_start` of virtual time, resumes the subscription's trigger
    async fn resume_webhook(&self, run_since_start: Duration) {
        self.fake_system_time_source.sleep(run_since_start).await;
        self.resume_flow(self.now(), &self.webhook_binding).await;
    }

    /// Known before the dataset is created, so mocks can be set up for it
    fn foo_alias_id() -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(b"foo")
    }

    fn head(n: u8) -> odf::Multihash {
        odf::Multihash::from_digest_sha3_256(format!("slice-{n}").as_bytes())
    }

    fn new_data_increment() -> MockDatasetIncrementQueryService {
        MockDatasetIncrementQueryService::with_increment_between(Self::ingest_increment())
    }

    fn ingest_increment() -> MetadataChainIncrementInterval {
        MetadataChainIncrementInterval {
            num_blocks: 1,
            num_records: 3,
            updated_watermark: None,
        }
    }

    fn target_url() -> url::Url {
        url::Url::parse("https://example.com/hook").unwrap()
    }

    fn unsuccessful_response(status_code: u16) -> TaskErrorWebhookDelivery {
        TaskErrorWebhookDelivery::UnsuccessfulResponse(WebhookUnsuccessfulResponseError {
            target_url: Self::target_url(),
            status_code,
        })
    }

    fn connection_timeout() -> TaskErrorWebhookDelivery {
        TaskErrorWebhookDelivery::ConnectionTimeout(WebhookSendConnectionTimeoutError {
            target_url: Self::target_url(),
            timeout: std::time::Duration::from_secs(10),
        })
    }

    fn failed_to_connect() -> TaskErrorWebhookDelivery {
        TaskErrorWebhookDelivery::FailedToConnect(WebhookSendFailedToConnectError {
            target_url: Self::target_url(),
        })
    }

    async fn push_over_http(
        &self,
        run_since_start: Duration,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) {
        self.fake_system_time_source.sleep(run_since_start).await;
        self.set_foo_head(new_head);
        self.issue_dataset_ingested_over_http(Duration::zero(), &self.foo_id, old_head, new_head)
            .await;
    }

    /// After `run_since_start` of virtual time, announces a force push that
    /// rewrote history. The increment service must reject the interval, so the
    /// change is seen as breaking.
    async fn force_push(
        &self,
        run_since_start: Duration,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) {
        self.fake_system_time_source.sleep(run_since_start).await;
        self.set_foo_head(new_head);

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
                DatasetExternallyChangedMessage::smart_transfer_protocol_sync(
                    &self.foo_id,
                    Some(old_head),
                    new_head,
                    None,
                    true,
                ),
            )
            .await
            .unwrap();
    }

    /// Lets "foo" be ingested, by hand in these tests
    async fn configure_ingest(&self) {
        self.set_dataset_flow_ingest(
            ingest_dataset_binding(&self.foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None,
        )
        .await;
    }

    fn manual_ingest_driver(&self, run_since_start: Duration) -> ManualFlowActivationDriver {
        self.manual_flow_trigger_driver(ManualFlowActivationArgs {
            flow_binding: ingest_dataset_binding(&self.foo_id),
            run_since_start,
            initiator_id: None,
            maybe_forced_flow_config_rule: None,
        })
    }

    /// Runs an ingest task of "foo" that moves its head 10ms after it starts
    async fn run_ingest_task(
        &self,
        task_id: u64,
        flow_id: u64,
        run_since_start: Duration,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) {
        let commit = async {
            self.fake_system_time_source
                .sleep(run_since_start + Duration::milliseconds(10))
                .await;
            self.set_foo_head(new_head);
        };
        let task_driver =
            self.ingest_task_driver(task_id, flow_id, run_since_start, old_head, new_head);

        tokio::join!(task_driver.run(), commit);
    }

    fn ingest_task_driver(
        &self,
        task_id: u64,
        flow_id: u64,
        run_since_start: Duration,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> TaskDriver {
        self.task_driver(TaskDriverArgs {
            task_id: TaskID::new(task_id),
            task_metadata: TaskMetadata::from(vec![(
                METADATA_TASK_FLOW_ID,
                FlowID::new(flow_id).to_string(),
            )]),
            dataset_id: Some(self.foo_id.clone()),
            run_since_start,
            finish_in_with: Some((
                Duration::milliseconds(10),
                TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                        pull_result: PullResult::Updated {
                            old_head: Some(old_head.clone()),
                            new_head: new_head.clone(),
                            has_more: false,
                        },
                        data_increment: Some(Self::ingest_increment()),
                    }
                    .into_task_result(),
                ),
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
                dataset_id: self.foo_id.clone(),
                fetch_uncacheable: false,
            }
            .into_logical_plan(),
        })
    }

    /// A delivery task of the given flow, checked against the expected plan
    fn delivery_task_driver(
        &self,
        task_id: u64,
        flow_id: u64,
        run_since_start: Duration,
        finish_in_with: (Duration, TaskOutcome),
        expected_logical_plan: LogicalPlan,
    ) -> TaskDriver {
        self.task_driver(TaskDriverArgs {
            task_id: TaskID::new(task_id),
            task_metadata: TaskMetadata::from(vec![(
                METADATA_TASK_FLOW_ID,
                FlowID::new(flow_id).to_string(),
            )]),
            dataset_id: None,
            run_since_start,
            finish_in_with: Some(finish_in_with),
            expected_logical_plan,
        })
    }

    fn deliver_plan(&self, old_head: &odf::Multihash, new_head: &odf::Multihash) -> LogicalPlan {
        self.webhook_deliver_plan(old_head, new_head, false)
    }

    fn deliver_breaking_change_plan(
        &self,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> LogicalPlan {
        self.webhook_deliver_plan(old_head, new_head, true)
    }

    fn webhook_deliver_plan(
        &self,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
        is_breaking_change: bool,
    ) -> LogicalPlan {
        let owner_id =
            odf::metadata::testing::account_id_by_maybe_name(&None, DEFAULT_ACCOUNT_NAME_STR);

        LogicalPlanWebhookDeliver {
            webhook_subscription_id: self.subscription_id,
            webhook_event_type: WebhookEventTypeCatalog::dataset_ref_updated(),
            webhook_payload: serde_json::json!({
                "version": 2,
                "dataset_id": self.foo_id.to_string(),
                "owner_account_id": owner_id.to_string(),
                "block_ref": "head",
                "new_hash": new_head.to_string(),
                "old_hash": old_head.to_string(),
                "is_breaking_change": is_breaking_change,
            }),
            webhook_watermark: Some(
                DatasetRefUpdatedWatermark {
                    head: new_head.clone(),
                }
                .into(),
            ),
        }
        .into_logical_plan()
    }

    async fn webhook_trigger_status(&self) -> Option<FlowTriggerStatus> {
        self.get_flow_trigger_status(&self.webhook_binding).await
    }

    fn flows_report(&self) -> String {
        let test_flow_listener = self.catalog.get_one::<FlowSystemTestListener>().unwrap();
        format!("{}", test_flow_listener.as_ref())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
