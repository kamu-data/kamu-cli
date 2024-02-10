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
use kamu_core::{auth, SystemTimeSource};
use kamu_flow_system::{FlowKey, FlowService};
use opendatafabric::{AccountName, FAKE_ACCOUNT_ID};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ManualFlowTriggerDriver {
    time_source: Arc<dyn SystemTimeSource>,
    flow_service: Arc<dyn FlowService>,
    args: ManualFlowTriggerArgs,
}

pub(crate) struct ManualFlowTriggerArgs {
    pub(crate) flow_key: FlowKey,
    pub(crate) run_since_start: Duration,
}

impl ManualFlowTriggerDriver {
    pub(crate) fn new(
        time_source: Arc<dyn SystemTimeSource>,
        flow_service: Arc<dyn FlowService>,
        args: ManualFlowTriggerArgs,
    ) -> Self {
        Self {
            time_source,
            flow_service,
            args,
        }
    }

    pub(crate) async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;

        self.flow_service
            .trigger_manual_flow(
                start_time + self.args.run_since_start,
                self.args.flow_key,
                FAKE_ACCOUNT_ID.to_string(),
                AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME),
            )
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
