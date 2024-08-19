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
use kamu_flow_system::{Flow, FlowEventStore, FlowID};
use kamu_task_system::TaskScheduler;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowAbortHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl FlowAbortHelper {
    pub(crate) fn new(
        flow_event_store: Arc<dyn FlowEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
    ) -> Self {
        Self {
            flow_event_store,
            time_source,
            task_scheduler,
        }
    }

    pub(crate) async fn abort_flow(&self, flow_id: FlowID) -> Result<(), InternalError> {
        // Mark flow as aborted
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .int_err()?;

        self.abort_loaded_flow(&mut flow).await
    }

    pub(crate) async fn abort_loaded_flow(&self, flow: &mut Flow) -> Result<(), InternalError> {
        // Abort flow itself
        flow.abort(self.time_source.now()).int_err()?;
        flow.save(self.flow_event_store.as_ref()).await.int_err()?;

        // Cancel associated tasks
        for task_id in &flow.task_ids {
            self.task_scheduler.cancel_task(*task_id).await.int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
