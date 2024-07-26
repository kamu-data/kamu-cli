// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Duration;
use kamu_task_system::*;
use messaging_outbox::{post_outbox_message, MessageRelevance, Outbox};
use opendatafabric::DatasetID;
use time_source::SystemTimeSource;
use tokio::task::yield_now;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct TaskDriver {
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
    args: TaskDriverArgs,
}

pub(crate) struct TaskDriverArgs {
    pub(crate) task_id: TaskID,
    pub(crate) dataset_id: Option<DatasetID>,
    pub(crate) run_since_start: Duration,
    pub(crate) finish_in_with: Option<(Duration, TaskOutcome)>,
    pub(crate) expected_logical_plan: LogicalPlan,
}

impl TaskDriver {
    pub(crate) fn new(
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
        task_event_store: Arc<dyn TaskSystemEventStore>,
        args: TaskDriverArgs,
    ) -> Self {
        Self {
            time_source,
            outbox,
            task_event_store,
            args,
        }
    }

    pub(crate) async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;
        while !(self.task_exists().await) {
            yield_now().await;
        }

        self.ensure_task_matches_logical_plan().await;

        // Note: we can omit transaction, since this is a test-only abstraction
        // with assummed immediate delivery
        post_outbox_message(
            self.outbox.as_ref(),
            MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
            TaskRunningMessage {
                event_time: start_time + self.args.run_since_start,
                task_id: self.args.task_id,
            },
            MessageRelevance::Essential,
        )
        .await
        .unwrap();

        if let Some((finish_in, with_outcome)) = self.args.finish_in_with {
            self.time_source.sleep(finish_in).await;

            // Note: we can omit transaction, since this is a test-only abstraction
            // with assummed immediate delivery
            post_outbox_message(
                self.outbox.as_ref(),
                MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                TaskFinishedMessage {
                    event_time: start_time + self.args.run_since_start + finish_in,
                    task_id: self.args.task_id,
                    outcome: with_outcome,
                },
                MessageRelevance::Essential,
            )
            .await
            .unwrap();
        }
    }

    async fn task_exists(&self) -> bool {
        Task::try_load(self.args.task_id, self.task_event_store.as_ref())
            .await
            .unwrap()
            .is_some()
    }

    async fn ensure_task_matches_logical_plan(&self) {
        let task = Task::load(self.args.task_id, self.task_event_store.as_ref())
            .await
            .expect("Task does not exist yet");

        assert_eq!(self.args.expected_logical_plan, task.logical_plan);
        match &task.logical_plan {
            LogicalPlan::UpdateDataset(ud) => {
                assert!(self.args.dataset_id.is_some());
                assert_eq!(&ud.dataset_id, self.args.dataset_id.as_ref().unwrap());
            }
            LogicalPlan::Probe(_) => assert!(self.args.dataset_id.is_none()),
            LogicalPlan::HardCompactionDataset(_) => (),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
