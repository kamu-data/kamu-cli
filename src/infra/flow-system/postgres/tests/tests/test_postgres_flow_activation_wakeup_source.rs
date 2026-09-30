// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use database_common::PostgresTransactionManager;
use database_common_macros::transactional_method1;
use dill::{Catalog, CatalogBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::*;
use kamu_flow_system_postgres::{PostgresFlowActivationWakeupSource, PostgresFlowEventStore};
use kamu_task_system::{TaskError, TaskID, TaskOutcome, TaskResult};
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::PgPool;
use wakeup_listener::{WakeHint, WakeupListener, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_wakes_up_only_when_flow_activation_is_scheduled(pg_pool: PgPool) {
    let harness = PostgresFlowActivationWakeupHarness::new(pg_pool);

    // Subscribing reports a possible change, then it's quiet
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // A new flow has no activation time yet
    let flow_id = harness.new_flow_id().await;
    harness.initiate_flow(flow_id).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    harness.schedule_for_activation(flow_id, Utc::now()).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);

    // Task scheduled, then running: activation time is reset, no reason to wake up
    harness.schedule_task(flow_id, TaskID::new(1)).await;
    harness.start_task(flow_id, TaskID::new(1)).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // A failed task with a planned retry sets a new activation time
    harness
        .finish_task(
            flow_id,
            TaskID::new(1),
            TaskOutcome::Failed(TaskError::empty_recoverable()),
            Some(Utc::now()),
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);

    // The retry succeeds: no new activation time
    harness.schedule_task(flow_id, TaskID::new(2)).await;
    harness.start_task(flow_id, TaskID::new(2)).await;
    harness
        .finish_task(
            flow_id,
            TaskID::new(2),
            TaskOutcome::Success(TaskResult::empty()),
            None,
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // Aborting a flow waiting for activation is not a reason to wake up
    let flow_id = harness.new_flow_id().await;
    harness.initiate_flow(flow_id).await;
    harness.schedule_for_activation(flow_id, Utc::now()).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    harness.abort_flow(flow_id).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // Another activation cause keeps the activation time: no reason to wake up
    let flow_id = harness.new_flow_id().await;
    harness.initiate_flow(flow_id).await;
    let activation_at = Utc::now() + TimeDelta::hours(1);
    harness
        .schedule_for_activation(flow_id, activation_at)
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    harness.add_activation_cause(flow_id).await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // Rescheduled earlier than the moment the agent may be waiting for
    harness
        .schedule_for_activation(flow_id, activation_at - TimeDelta::minutes(30))
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresFlowActivationWakeupHarness {
    catalog: Catalog,
    wakeup_listener: Box<dyn WakeupListener>,
    flow_binding: FlowBinding,
    last_event_ids: Mutex<HashMap<FlowID, EventID>>,
}

impl PostgresFlowActivationWakeupHarness {
    fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresFlowEventStore>();
        catalog_builder.add::<PostgresFlowActivationWakeupSource>();
        catalog_builder.add::<PostgresNotificationHub>();
        catalog_builder.add::<WakeupListenerMetrics>();

        let catalog = catalog_builder.build();

        let wakeup_listener = catalog
            .get_one::<dyn FlowActivationWakeupSource>()
            .unwrap()
            .new_wakeup_listener();

        Self {
            catalog,
            wakeup_listener,
            flow_binding: FlowBinding::new("dev.kamu.flow.test", FlowScope::make_system_scope()),
            last_event_ids: Default::default(),
        }
    }

    async fn wait_wake(&self) -> WakeHint {
        self.wakeup_listener
            .wait_wake(Duration::from_millis(500), Duration::from_millis(10))
            .await
            .unwrap()
    }

    async fn new_flow_id(&self) -> FlowID {
        self.new_flow_id_impl().await.unwrap()
    }

    async fn initiate_flow(&self, flow_id: FlowID) {
        self.save_event(
            flow_id,
            FlowEventInitiated {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                activation_cause: FlowActivationCause::AutoPolling(
                    FlowActivationCauseAutoPolling {
                        activation_time: Utc::now(),
                    },
                ),
                config_snapshot: None,
                retry_policy: None,
            }
            .into(),
        )
        .await;
    }

    async fn schedule_for_activation(&self, flow_id: FlowID, activation_at: DateTime<Utc>) {
        self.save_event(
            flow_id,
            FlowEventScheduledForActivation {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                scheduled_for_activation_at: activation_at,
            }
            .into(),
        )
        .await;
    }

    async fn add_activation_cause(&self, flow_id: FlowID) {
        self.save_event(
            flow_id,
            FlowEventActivationCauseAdded {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                activation_cause: FlowActivationCause::AutoPolling(
                    FlowActivationCauseAutoPolling {
                        activation_time: Utc::now(),
                    },
                ),
            }
            .into(),
        )
        .await;
    }

    async fn schedule_task(&self, flow_id: FlowID, task_id: TaskID) {
        self.save_event(
            flow_id,
            FlowEventTaskScheduled {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                task_id,
            }
            .into(),
        )
        .await;
    }

    async fn start_task(&self, flow_id: FlowID, task_id: TaskID) {
        self.save_event(
            flow_id,
            FlowEventTaskRunning {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                task_id,
            }
            .into(),
        )
        .await;
    }

    async fn finish_task(
        &self,
        flow_id: FlowID,
        task_id: TaskID,
        task_outcome: TaskOutcome,
        next_attempt_at: Option<DateTime<Utc>>,
    ) {
        self.save_event(
            flow_id,
            FlowEventTaskFinished {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
                task_id,
                task_outcome,
                next_attempt_at,
            }
            .into(),
        )
        .await;
    }

    async fn abort_flow(&self, flow_id: FlowID) {
        self.save_event(
            flow_id,
            FlowEventAborted {
                event_time: Utc::now(),
                flow_id,
                flow_binding: self.flow_binding.clone(),
            }
            .into(),
        )
        .await;
    }

    async fn save_event(&self, flow_id: FlowID, event: FlowEvent) {
        let maybe_prev_stored_event_id = self.last_event_ids.lock().unwrap().get(&flow_id).copied();
        let last_event_id = self
            .save_event_impl(flow_id, maybe_prev_stored_event_id, event)
            .await
            .unwrap();
        self.last_event_ids
            .lock()
            .unwrap()
            .insert(flow_id, last_event_id);
    }

    #[transactional_method1(flow_event_store: Arc<dyn FlowEventStore>)]
    async fn new_flow_id_impl(&self) -> Result<FlowID, InternalError> {
        flow_event_store.new_flow_id().await
    }

    #[transactional_method1(flow_event_store: Arc<dyn FlowEventStore>)]
    async fn save_event_impl(
        &self,
        flow_id: FlowID,
        maybe_prev_stored_event_id: Option<EventID>,
        event: FlowEvent,
    ) -> Result<EventID, InternalError> {
        flow_event_store
            .save_events(&flow_id, maybe_prev_stored_event_id, vec![event])
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
