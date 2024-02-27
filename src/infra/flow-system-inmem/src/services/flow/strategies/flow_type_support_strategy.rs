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
use kamu_flow_system::{BatchingRule, FlowID, FlowKey, FlowResult, FlowState};

use super::FlowServiceCallbacksFacade;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait FlowTypeSupportStrategy: Send + Sync {
    async fn on_flow_success(
        &self,
        facade: &dyn FlowServiceCallbacksFacade,
        success_time: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: &FlowKey,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError>;

    async fn evaluate_batching_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow_state: &FlowState,
        batching_rule: &BatchingRule,
    ) -> Result<BatchingRuleEvaluation, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct BatchingRuleEvaluation {
    pub batching_deadline: DateTime<Utc>,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
    pub satisfied: bool,
}

impl BatchingRuleEvaluation {
    pub(crate) fn accumulated_something(&self) -> bool {
        self.accumulated_records_count > 0 || self.watermark_modified
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
