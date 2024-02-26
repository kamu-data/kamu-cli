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
use kamu_core::InternalError;
use kamu_flow_system::{BatchingRule, FlowState};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait BatchingRuleEvaluator: Send + Sync {
    async fn evaluate(
        &self,
        evaluation_time: DateTime<Utc>,
        flow_state: &FlowState,
        batching_rule: &BatchingRule,
    ) -> Result<BatchingRuleEvaluationResult, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) enum BatchingRuleEvaluationResult {
    Inapplicable,
    Success(BatchingRuleEvaluation),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct BatchingRuleEvaluation {
    pub batching_deadline: DateTime<Utc>,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
    pub satisfied: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn evaluate_batching_rules(
    evaluation_time: DateTime<Utc>,
    flow_state: &FlowState,
    batching_rule: &BatchingRule,
    evaluators: &[Arc<dyn BatchingRuleEvaluator>],
) -> Result<BatchingRuleEvaluationResult, InternalError> {
    for evaluator in evaluators {
        match evaluator
            .evaluate(evaluation_time, flow_state, batching_rule)
            .await?
        {
            BatchingRuleEvaluationResult::Inapplicable => {}
            BatchingRuleEvaluationResult::Success(evaluation) => {
                return Ok(BatchingRuleEvaluationResult::Success(evaluation))
            }
        }
    }
    Ok(BatchingRuleEvaluationResult::Inapplicable)
}

/////////////////////////////////////////////////////////////////////////////////////////
