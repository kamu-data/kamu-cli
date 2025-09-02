// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{self as fs, Flow};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowPeriodicProcess {
    flow_trigger: fs::FlowTriggerState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl FlowPeriodicProcess {
    #[graphql(skip)]
    pub fn new(flow_trigger: fs::FlowTriggerState) -> Self {
        Self { flow_trigger }
    }

    #[graphql(skip)]
    #[inline]
    pub fn flow_binding(&self) -> &fs::FlowBinding {
        &self.flow_trigger.flow_binding
    }

    async fn flow_type(&self) -> DatasetFlowType {
        decode_dataset_flow_type(&self.flow_trigger.flow_binding.flow_type)
    }

    async fn flow_trigger(&self) -> FlowTrigger {
        self.flow_trigger.clone().into()
    }

    async fn runtime_state(&self, ctx: &Context<'_>) -> Result<FlowPeriodicProcessState> {
        let flow_event_store = from_catalog_n!(ctx, dyn fs::FlowEventStore);

        let consecutive_failures = flow_event_store
            .get_current_consecutive_flow_failures_count(self.flow_binding())
            .await?;

        let effective_status = match self.flow_trigger.status {
            fs::FlowTriggerStatus::Active => {
                if consecutive_failures == 0 {
                    FlowPeriodicProcessStatus::Active
                } else {
                    FlowPeriodicProcessStatus::Failing
                }
            }
            fs::FlowTriggerStatus::PausedByUser => FlowPeriodicProcessStatus::PausedByUser,
            fs::FlowTriggerStatus::StoppedAutomatically => {
                FlowPeriodicProcessStatus::StoppedAutomatically
            }
            fs::FlowTriggerStatus::ScopeRemoved => {
                unreachable!("ScopeRemoved triggers are not expected here")
            }
        };

        let run_stats = flow_event_store
            .get_flow_run_stats(self.flow_binding())
            .await?;

        let maybe_pending_flow_id = flow_event_store
            .try_get_pending_flow(self.flow_binding())
            .await?;
        let next_planned_at = if let Some(pending_flow_id) = maybe_pending_flow_id {
            let pending_flow = Flow::load(pending_flow_id, flow_event_store.as_ref())
                .await
                .int_err()?;
            pending_flow.timing.scheduled_for_activation_at
        } else {
            None
        };

        Ok(FlowPeriodicProcessState {
            effective_status,
            consecutive_failures,
            last_success_at: run_stats.last_success_time,
            last_attempt_at: run_stats.last_attempt_time,
            last_failure_at: run_stats.last_failure_time,
            next_planned_at,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
