// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use database_common_macros::transactional_method1;
use dill::Catalog;
use kamu_accounts::DEFAULT_ACCOUNT_ID;
use kamu_flow_system::{FlowBinding, FlowConfigurationRule, FlowRunService, RunFlowError};
use kamu_task_system::TaskRunArguments;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ManualFlowActivationDriver {
    catalog: Catalog,
    time_source: Arc<dyn SystemTimeSource>,
    args: ManualFlowActivationArgs,
}

pub(crate) struct ManualFlowActivationArgs {
    pub(crate) flow_binding: FlowBinding,
    pub(crate) run_since_start: Duration,
    pub(crate) initiator_id: Option<odf::AccountID>,
    pub(crate) maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    pub(crate) maybe_task_run_arguments: Option<TaskRunArguments>,
}

impl ManualFlowActivationDriver {
    pub(crate) fn new(
        catalog: Catalog,
        time_source: Arc<dyn SystemTimeSource>,
        args: ManualFlowActivationArgs,
    ) -> Self {
        Self {
            catalog,
            time_source,
            args,
        }
    }

    pub(crate) async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;

        self.send_trigger_manual_flow(start_time).await.unwrap();
    }

    #[transactional_method1(flow_run_service: Arc<dyn FlowRunService>)]
    async fn send_trigger_manual_flow(
        &self,
        start_time: DateTime<Utc>,
    ) -> Result<(), RunFlowError> {
        flow_run_service
            .run_flow_manually(
                start_time + self.args.run_since_start,
                &self.args.flow_binding,
                self.args
                    .initiator_id
                    .clone()
                    .unwrap_or(DEFAULT_ACCOUNT_ID.clone()),
                self.args.maybe_forced_flow_config_rule.clone(),
                self.args.maybe_task_run_arguments.clone(),
            )
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
