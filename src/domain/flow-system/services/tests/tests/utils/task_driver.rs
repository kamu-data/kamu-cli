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
use kamu_adapter_task_webhook::LogicalPlanWebhookDeliver;
use kamu_task_system::*;
use kamu_webhooks::WebhookWatermarkRepository;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;
use tokio::task::yield_now;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct TaskDriver {
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
    task_event_store: Arc<dyn TaskEventStore>,
    webhook_watermark_repo: Arc<dyn WebhookWatermarkRepository>,
    args: TaskDriverArgs,
}

#[derive(Debug)]
pub(crate) struct TaskDriverArgs {
    pub(crate) task_id: TaskID,
    pub(crate) task_metadata: TaskMetadata,
    pub(crate) dataset_id: Option<odf::DatasetID>,
    pub(crate) run_since_start: Duration,
    pub(crate) finish_in_with: Option<(Duration, TaskOutcome)>,
    pub(crate) expected_logical_plan: LogicalPlan,
}

impl TaskDriver {
    pub(crate) fn new(
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
        task_event_store: Arc<dyn TaskEventStore>,
        webhook_watermark_repo: Arc<dyn WebhookWatermarkRepository>,
        args: TaskDriverArgs,
    ) -> Self {
        Self {
            time_source,
            outbox,
            task_event_store,
            webhook_watermark_repo,
            args,
        }
    }

    pub(crate) async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;
        while !self.task_exists().await {
            yield_now().await;
        }

        self.ensure_task_matches_logical_plan().await;

        // Note: We can omit transaction, since this is a test-only abstraction
        //       with assumed immediate delivery
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                TaskProgressMessage::running(
                    start_time + self.args.run_since_start,
                    self.args.task_id,
                    self.args.task_metadata.clone(),
                ),
            )
            .await
            .unwrap();

        if let Some((finish_in, with_outcome)) = self.args.finish_in_with {
            self.time_source.sleep(finish_in).await;

            if let TaskOutcome::Success(_) = &with_outcome {
                Self::record_delivered_webhook_watermark(
                    self.webhook_watermark_repo.as_ref(),
                    &self.args.expected_logical_plan,
                )
                .await;
            }

            // Note: We can omit transaction, since this is a test-only abstraction
            //       with assumed immediate delivery
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                    TaskProgressMessage::finished(
                        start_time + self.args.run_since_start + finish_in,
                        self.args.task_id,
                        self.args.task_metadata,
                        with_outcome,
                    ),
                )
                .await
                .unwrap();
        }
    }

    /// Moves the watermark of a successful webhook delivery, as the delivery
    /// worker does before the task finishes
    async fn record_delivered_webhook_watermark(
        webhook_watermark_repo: &dyn WebhookWatermarkRepository,
        logical_plan: &LogicalPlan,
    ) {
        if logical_plan.plan_type != LogicalPlanWebhookDeliver::TYPE_ID {
            return;
        }

        let plan = LogicalPlanWebhookDeliver::from_logical_plan(logical_plan).unwrap();
        if let Some(watermark) = plan.webhook_watermark {
            webhook_watermark_repo
                .set_watermark(
                    plan.webhook_subscription_id,
                    &plan.webhook_event_type,
                    watermark,
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

        pretty_assertions::assert_eq!(self.args.expected_logical_plan, task.logical_plan);
        pretty_assertions::assert_eq!(self.args.dataset_id, task.logical_plan.dataset_id());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
