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
use database_common_macros::transactional_method1;
use dill::Catalog;
use kamu_flow_system::{CancelScheduledTasksError, FlowID, FlowRunService};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ManualFlowAbortDriver {
    catalog: Catalog,
    time_source: Arc<dyn SystemTimeSource>,
    args: ManualFlowAbortArgs,
}

pub(crate) struct ManualFlowAbortArgs {
    pub(crate) flow_id: FlowID,
    pub(crate) abort_since_start: Duration,
}

impl ManualFlowAbortDriver {
    pub(crate) fn new(
        catalog: Catalog,
        time_source: Arc<dyn SystemTimeSource>,
        args: ManualFlowAbortArgs,
    ) -> Self {
        Self {
            catalog,
            time_source,
            args,
        }
    }

    pub(crate) async fn run(self) {
        self.time_source.sleep(self.args.abort_since_start).await;

        self.send_abort_flow().await.unwrap();
    }

    #[transactional_method1(flow_run_service: Arc<dyn FlowRunService>)]
    async fn send_abort_flow(&self) -> Result<(), CancelScheduledTasksError> {
        flow_run_service
            .cancel_scheduled_tasks(self.args.flow_id)
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
