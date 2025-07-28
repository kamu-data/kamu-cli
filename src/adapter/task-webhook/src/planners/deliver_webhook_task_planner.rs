// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_task_system::*;

use crate::{LogicalPlanWebhookDeliver, TaskDefinitionWebhookDeliver};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanWebhookDeliver::TYPE_ID,
})]
pub struct DeliverWebhookTaskPlanner {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for DeliverWebhookTaskPlanner {
    async fn prepare_task_definition(
        &self,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanWebhookDeliver::TYPE_ID,
            "DeliverWebhookTaskPlanner received an unsupported logical plan type: {logical_plan:?}",
        );

        let webhook_plan = LogicalPlanWebhookDeliver::from_logical_plan(logical_plan)?;

        Ok(TaskDefinition::new(TaskDefinitionWebhookDeliver {
            task_id,
            webhook_subscription_id: webhook_plan.webhook_subscription_id,
            webhook_event_type: webhook_plan.webhook_event_type,
            webhook_payload: webhook_plan.webhook_payload,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
