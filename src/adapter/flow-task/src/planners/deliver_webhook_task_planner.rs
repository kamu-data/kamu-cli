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

use crate::TaskDefinitionWebhookDeliver;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
pub struct DeliverWebhookTaskPlanner {}

impl DeliverWebhookTaskPlanner {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for DeliverWebhookTaskPlanner {
    fn supported_task_type(&self) -> &str {
        TaskDefinitionWebhookDeliver::TASK_TYPE
    }

    async fn prepare_task_definition(
        &self,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        let kamu_task_system::LogicalPlan::DeliverWebhook(webhook_plan) = logical_plan else {
            panic!(
                "DeliverWebhookTaskPlanner received an unsupported logical plan type: \
                 {logical_plan:?}",
            );
        };

        Ok(TaskDefinition::new(TaskDefinitionWebhookDeliver {
            task_id,
            webhook_subscription_id: webhook_plan.webhook_subscription_id,
            webhook_event_id: webhook_plan.webhook_event_id,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
