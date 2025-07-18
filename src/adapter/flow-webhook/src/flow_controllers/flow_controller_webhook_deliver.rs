// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use internal_error::InternalError;
use kamu_webhooks::{
    WebhookEventTypeCatalog,
    WebhookSubscriptionID,
    WebhookSubscriptionQueryService,
};
use {kamu_adapter_task_webhook as atw, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{DatasetUpdatedWebhookSensor, FLOW_TYPE_WEBHOOK_DELIVER};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_WEBHOOK_DELIVER,
})]
pub struct FlowControllerWebhookDeliver {
    webhook_subscription_query_service: Arc<dyn WebhookSubscriptionQueryService>,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerWebhookDeliver {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_WEBHOOK_DELIVER
    }

    async fn register_flow_sensor(
        &self,
        flow_binding: &fs::FlowBinding,
        _: fs::FlowTriggerRule,
    ) -> Result<(), InternalError> {
        let webhook_subscription_id = flow_binding.get_webhook_subscription_id_or_die()?;

        let webhook_subscription = self
            .webhook_subscription_query_service
            .find_webhook_subscription(WebhookSubscriptionID::new(webhook_subscription_id))
            .await?
            .ok_or_else(|| {
                InternalError::new(format!(
                    "Webhook subscription with ID '{webhook_subscription_id}' not found"
                ))
            })?;

        if webhook_subscription.dataset_id().is_some()
            && webhook_subscription
                .expects_event_type(&WebhookEventTypeCatalog::dataset_ref_updated())
        {
            // TODO: should we respect a BatchingRule here?
            self.flow_sensor_dispatcher
                .register_sensor(Arc::new(DatasetUpdatedWebhookSensor::new(
                    &webhook_subscription,
                )))
                .await?;
        }

        Ok(())
    }

    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        _maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let subscription_id = flow_binding.get_webhook_subscription_id_or_die()?;

        let delivery_args = if let Some(task_run_arguments) = maybe_task_run_arguments
            && task_run_arguments.arguments_type == atw::TaskRunArgumentsWebhookDeliver::TYPE_ID
        {
            atw::TaskRunArgumentsWebhookDeliver::from_task_run_arguments(task_run_arguments)?
        } else {
            return InternalError::bail("Webhook delivery flow cannot be called without arguments");
        };

        Ok(atw::LogicalPlanWebhookDeliver {
            webhook_subscription_id: subscription_id,
            webhook_event_type: delivery_args.event_type,
            webhook_payload: delivery_args.payload,
        }
        .into_logical_plan())
    }

    async fn propagate_success(
        &self,
        _: &fs::FlowState,
        _: &ts::TaskResult,
        _: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // No further actions triggered with a webhook delivery
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
