// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::{component, interface};
use kamu_flow_system::*;

use crate::{FlowAbortHelper, FlowSchedulingHelper};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowRunService)]
pub struct FlowRunServiceImpl {
    flow_scheduling_helper: Arc<FlowSchedulingHelper>,
    flow_abort_helper: Arc<FlowAbortHelper>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_event_store: Arc<dyn FlowEventStore>,

    agent_config: Arc<FlowAgentConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowRunService for FlowRunServiceImpl {
    /// Triggers the specified flow manually, unless it's already waiting
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(?flow_binding, %initiator_account_id)
    )]
    async fn run_flow_manually(
        &self,
        activation_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        initiator_account_id: odf::AccountID,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, RunFlowError> {
        let activation_time = self.agent_config.round_time(activation_time)?;

        self.flow_scheduling_helper
            .trigger_flow_common(
                activation_time,
                flow_binding,
                None,
                vec![FlowActivationCause::Manual(FlowActivationCauseManual {
                    activation_time,
                    initiator_account_id,
                })],
                maybe_forced_flow_config_rule,
            )
            .await
            .map_err(Into::into)
    }

    /// Triggers the specified flow with custom trigger instance,
    /// unless it's already waiting
    async fn run_flow_automatically(
        &self,
        activation_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        activation_causes: Vec<FlowActivationCause>,
        maybe_flow_trigger_rule: Option<FlowTriggerRule>,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, RunFlowError> {
        self.flow_scheduling_helper
            .trigger_flow_common(
                activation_time,
                flow_binding,
                maybe_flow_trigger_rule,
                activation_causes,
                maybe_forced_flow_config_rule,
            )
            .await
            .map_err(Into::into)
    }

    /// Attempts to cancel the tasks already scheduled for the given flow
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(%flow_id)
    )]
    async fn cancel_flow_run(
        &self,
        cancellation_time: DateTime<Utc>,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelFlowRunError> {
        // Load flow to ensure it exists
        let mut flow = match Flow::load(flow_id, self.flow_event_store.as_ref()).await {
            Ok(flow) => flow,
            Err(LoadError::NotFound(_)) => {
                return Err(CancelFlowRunError::NotFound(FlowNotFoundError { flow_id }));
            }
            Err(LoadError::ProjectionError(e)) => {
                return Err(CancelFlowRunError::Internal(e.int_err()));
            }
            Err(LoadError::Internal(e)) => {
                return Err(CancelFlowRunError::Internal(e));
            }
        };

        // Abort current flow and it's scheduled tasks
        self.flow_abort_helper.abort_flow(&mut flow).await?;

        // Find a trigger and pause it if it's periodic
        let maybe_active_schedule = self
            .flow_trigger_service
            .try_get_flow_active_schedule_rule(&flow.flow_binding)
            .await?;
        if maybe_active_schedule.is_some() {
            // TODO: avoid double-loading the trigger
            self.flow_trigger_service
                .pause_flow_trigger(cancellation_time, &flow.flow_binding) // pause (user), not stop (system)
                .await
                .int_err()?;
        }

        Ok(flow.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
