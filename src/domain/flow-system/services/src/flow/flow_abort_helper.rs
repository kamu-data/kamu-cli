// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::*;
use kamu_task_system::TaskScheduler;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowScopeRemovalHandler)]
pub(crate) struct FlowAbortHelper {
    catalog: dill::Catalog,
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_sensor_dispatcher: Arc<dyn FlowSensorDispatcher>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowAbortHelper {
    pub(crate) async fn abort_flow(&self, flow: &mut Flow) -> Result<(), InternalError> {
        // Mark flow as aborted
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

        Ok(())
    }

    pub(crate) async fn deactivate_flow_trigger(
        &self,
        target_catalog: &dill::Catalog,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        tracing::trace!(?flow_binding, "Deactivating flow trigger");

        let maybe_pending_flow_id = {
            let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();
            flow_event_store.try_get_pending_flow(flow_binding).await?
        };

        if let Some(flow_id) = maybe_pending_flow_id {
            let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                .await
                .int_err()?;

            self.abort_flow(&mut flow).await?;
        }

        self.flow_sensor_dispatcher
            .unregister_sensor(&flow_binding.scope)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowAbortHelper {
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let flow_event_store = self.catalog.get_one::<dyn FlowEventStore>().unwrap();

        // Query flows by this scope
        let flow_ids_2_abort = flow_event_store
            .try_get_all_scope_pending_flows(flow_scope)
            .await?;

        // Load these flows
        let flows_2_abort: Vec<Flow> =
            Flow::load_multi_simple(flow_ids_2_abort, flow_event_store.as_ref())
                .await
                .int_err()?;

        // Abort matched flows
        for mut flow in flows_2_abort {
            self.abort_flow(&mut flow).await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
