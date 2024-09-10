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
use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use kamu_accounts::DEFAULT_ACCOUNT_ID;
use kamu_flow_system::{FlowKey, FlowQueryService};
use opendatafabric::AccountID;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ManualFlowTriggerDriver {
    catalog: Catalog,
    time_source: Arc<dyn SystemTimeSource>,
    args: ManualFlowTriggerArgs,
}

pub(crate) struct ManualFlowTriggerArgs {
    pub(crate) flow_key: FlowKey,
    pub(crate) run_since_start: Duration,
    pub(crate) initiator_id: Option<AccountID>,
}

impl ManualFlowTriggerDriver {
    pub(crate) fn new(
        catalog: Catalog,
        time_source: Arc<dyn SystemTimeSource>,
        args: ManualFlowTriggerArgs,
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

        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional(
                "ManualFlowTriggerDriver::trigger_manual_flow",
                |transactional_catalog| async move {
                    let flow_query_service = transactional_catalog
                        .get_one::<dyn FlowQueryService>()
                        .unwrap();
                    flow_query_service
                        .trigger_manual_flow(
                            start_time + self.args.run_since_start,
                            self.args.flow_key,
                            self.args.initiator_id.unwrap_or(DEFAULT_ACCOUNT_ID.clone()),
                            None,
                        )
                        .await
                },
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
