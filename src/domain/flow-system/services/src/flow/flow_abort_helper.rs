// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{Flow, FlowEventStore, FlowID, FlowProgressMessage, FlowState, FlowStatus};
use kamu_task_system::TaskScheduler;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use crate::MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowAbortHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl FlowAbortHelper {
    pub(crate) fn new(
        flow_event_store: Arc<dyn FlowEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            flow_event_store,
            time_source,
            task_scheduler,
            outbox,
        }
    }

    pub(crate) async fn abort_flow(&self, flow_id: FlowID) -> Result<FlowState, InternalError> {
        // Mark flow as aborted
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .int_err()?;

        match flow.status() {
            FlowStatus::Waiting | FlowStatus::Retrying | FlowStatus::Running => {
                // Abort flow itself
                flow.abort(self.time_source.now()).int_err()?;
                flow.save(self.flow_event_store.as_ref()).await.int_err()?;

                // Cancel associated tasks
                for task_id in &flow.task_ids {
                    self.task_scheduler.cancel_task(*task_id).await.int_err()?;
                }

                // Notify the flow has been aborted
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                        FlowProgressMessage::cancelled(self.time_source.now(), flow.flow_id),
                    )
                    .await?;
            }
            FlowStatus::Finished => {
                /* Skip, idempotence */
                tracing::info!(
                    flow_id = %flow.flow_id,
                    flow_status = %flow.status(),
                    "Flow abortion skipped as no longer relevant"
                );
            }
        }

        Ok(flow.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
