// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::InternalError;
use kamu_flow_system::{BatchingRule, DatasetFlowType, FlowKey, FlowTrigger};
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait FlowServiceCallbacksFacade: Send + Sync {
    fn try_get_dataset_batching_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<BatchingRule>;

    async fn trigger_flow(
        &self,
        trigger_time: DateTime<Utc>,
        flow_key: &FlowKey,
        trigger: FlowTrigger,
        maybe_batching_rule: Option<&BatchingRule>,
    ) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
