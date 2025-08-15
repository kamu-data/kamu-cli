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
        flow_binding: &FlowBinding,
        activation_causes: Vec<FlowActivationCause>,
        maybe_flow_trigger_rule: Option<FlowTriggerRule>,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, RunFlowError> {
        self.flow_scheduling_helper
            .trigger_flow_common(
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
    async fn cancel_scheduled_tasks(
        &self,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelScheduledTasksError> {
        // Abort current flow and it's scheduled tasks
        self.flow_abort_helper
            .abort_flow(flow_id)
            .await
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
