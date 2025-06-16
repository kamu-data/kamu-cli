// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_task_system as ts;
use kamu_webhooks::{WebhookEventID, WebhookSubscriptionID, WebhookTaskFactory};

use crate::LogicalPlanWebhookDeliver;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn WebhookTaskFactory)]
pub struct WebhookTaskFactoryImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookTaskFactory for WebhookTaskFactoryImpl {
    async fn build_delivery_task_plan(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_id: WebhookEventID,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let plan = LogicalPlanWebhookDeliver {
            webhook_subscription_id: subscription_id.into_inner(),
            webhook_event_id: event_id.into_inner(),
        };

        Ok(ts::LogicalPlan {
            plan_type: LogicalPlanWebhookDeliver::SERIALIZATION_TYPE_ID.to_string(),
            payload: serde_json::to_value(plan).int_err()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
