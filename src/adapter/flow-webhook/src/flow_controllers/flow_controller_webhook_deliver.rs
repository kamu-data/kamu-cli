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
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, DatasetResourceUpdateDetails};
use kamu_adapter_task_webhook as atw;
use kamu_datasets::{DatasetEntryService, DatasetIncrementQueryService, DatasetReferenceService};
use kamu_flow_system as fs;
use kamu_task_system as ts;
use kamu_webhooks::{
    ResultIntoInternal,
    WebhookEventType,
    WebhookEventTypeCatalog,
    WebhookWatermark,
};

use crate::{
    DatasetRefUpdatedWatermark,
    DatasetRefUpdatedWatermarkProvider,
    DatasetUpdatedWebhookSensor,
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
    WEBHOOK_DATASET_REF_UPDATED_VERSION,
    WebhookDatasetRefUpdatedPayload,
    dataset_changes_between,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_WEBHOOK_DELIVER,
})]
pub struct FlowControllerWebhookDeliver {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dataset_reference_service: Arc<dyn DatasetReferenceService>,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
    watermark_provider: Arc<DatasetRefUpdatedWatermarkProvider>,
}

/// Webhook payload with the watermark to record once the receiver accepts it
struct WebhookDeliveryContent {
    payload: serde_json::Value,
    watermark: Option<WebhookWatermark>,
}

impl FlowControllerWebhookDeliver {
    async fn build_payload(
        &self,
        event_type: &WebhookEventType,
        maybe_dataset_id: Option<&odf::DatasetID>,
        flow: &fs::FlowState,
    ) -> Result<WebhookDeliveryContent, InternalError> {
        if event_type.as_ref() == WebhookEventTypeCatalog::DATASET_REF_UPDATED {
            let input_dataset_id = maybe_dataset_id.ok_or_else(|| {
                InternalError::new(format!(
                    "Dataset ID is required for {} event",
                    event_type.as_ref()
                ))
            })?;
            self.build_dataset_ref_updated_payload(flow, input_dataset_id)
                .await
        } else {
            Err(InternalError::new(format!(
                "FlowControllerWebhookDeliver does not support event type: {event_type}"
            )))
        }
    }

    async fn build_dataset_ref_updated_payload(
        &self,
        flow: &fs::FlowState,
        input_dataset_id: &odf::DatasetID,
    ) -> Result<WebhookDeliveryContent, InternalError> {
        // Find out who is the owner of the dataset
        let dataset_entry = self
            .dataset_entry_service
            .get_entry(input_dataset_id)
            .await
            .int_err()?;

        // The causes are dataset updates; the first one's old head is where the
        // receiver stands when there is no watermark
        let mut first_old_head: Option<Option<odf::Multihash>> = None;
        for activation_cause in &flow.activation_causes {
            if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
                if update.resource_type != DATASET_RESOURCE_TYPE {
                    return Err(InternalError::new(format!(
                        "Unexpected resource type in webhook activation cause: {}",
                        update.resource_type
                    )));
                }
                let dataset_update_details =
                    serde_json::from_value::<DatasetResourceUpdateDetails>(update.details.clone())
                        .int_err()?;

                if &dataset_update_details.dataset_id != input_dataset_id {
                    return Err(InternalError::new(format!(
                        "Dataset ID in update details {} does not match input dataset ID \
                         {input_dataset_id}",
                        dataset_update_details.dataset_id
                    )));
                }

                first_old_head.get_or_insert(dataset_update_details.old_head_maybe);
            }
        }
        let first_old_head = first_old_head.ok_or_else(|| {
            InternalError::new("No dataset updates found in the flow state for the input dataset")
        })?;

        // Report the dataset as it is now, from the head the receiver already has:
        // changes lost with a failed delivery, or announced out of order, are covered
        let new_head = self
            .dataset_reference_service
            .get_reference(input_dataset_id, &odf::BlockRef::Head)
            .await
            .int_err()?;
        let subscription_id =
            FlowScopeSubscription::new(&flow.flow_binding.scope).subscription_id();
        let old_head = self
            .watermark_provider
            .find_delivered_head(subscription_id)
            .await?
            .or(first_old_head);

        let is_breaking_change = match &old_head {
            Some(old_head) => match dataset_changes_between(
                self.dataset_increment_query_service.as_ref(),
                input_dataset_id,
                old_head,
                &new_head,
            )
            .await?
            {
                fs::ResourceChanges::NewData(_) => false,
                fs::ResourceChanges::Breaking => true,
            },
            None => false,
        };

        // Build webhook payload
        let webhook_payload = serde_json::to_value(WebhookDatasetRefUpdatedPayload {
            version: WEBHOOK_DATASET_REF_UPDATED_VERSION,
            dataset_id: input_dataset_id.to_string(),
            owner_account_id: dataset_entry.owner_id.to_string(),
            block_ref: odf::BlockRef::Head.to_string(),
            new_hash: new_head.to_string(),
            old_hash: old_head.map(|h| h.to_string()),
            is_breaking_change,
        })
        .int_err()?;

        Ok(WebhookDeliveryContent {
            payload: webhook_payload,
            watermark: Some(DatasetRefUpdatedWatermark { head: new_head }.into()),
        })
    }
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerWebhookDeliver {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_WEBHOOK_DELIVER
    }

    #[tracing::instrument(name = "FlowControllerWebhookDeliver::ensure_flow_sensor", skip_all)]
    async fn ensure_flow_sensor(
        &self,
        flow_binding: &fs::FlowBinding,
        activation: fs::FlowSensorActivation,
        reactive_rule: fs::ReactiveRule,
    ) -> Result<(), InternalError> {
        let subscription_scope = FlowScopeSubscription::new(&flow_binding.scope);
        if subscription_scope.event_type() == WebhookEventTypeCatalog::dataset_ref_updated() {
            // Attempt lookup for existing sensor
            if let Some(sensor) = self
                .flow_sensor_dispatcher
                .find_sensor(&flow_binding.scope)
                .await
            {
                tracing::debug!(scope=?flow_binding.scope, rule=?reactive_rule, "Updating existing sensor");

                // Sensor already exists, update its rule
                sensor.update_rule(reactive_rule);
                return Ok(());
            }

            // Create and register a new sensor

            tracing::info!(scope=?flow_binding.scope, rule=?reactive_rule, "Registering new sensor");

            let sensor = Arc::new(DatasetUpdatedWebhookSensor::new(
                flow_binding.scope.clone(),
                reactive_rule,
            ));

            self.flow_sensor_dispatcher
                .register_sensor(&self.catalog, activation, sensor)
                .await?;
        } else {
            tracing::error!(
                "FlowControllerWebhookDeliver does not support event type: {}",
                subscription_scope.event_type()
            );
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "FlowControllerWebhookDeliver::build_task_logical_plan",
        skip_all
    )]
    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let subscription_scope = FlowScopeSubscription::new(&flow.flow_binding.scope);

        let subscription_id = subscription_scope.subscription_id();
        let event_type = subscription_scope.event_type();
        let maybe_dataset_id = subscription_scope.maybe_dataset_id();

        let content = self
            .build_payload(&event_type, maybe_dataset_id.as_ref(), flow)
            .await?;

        Ok(atw::LogicalPlanWebhookDeliver {
            webhook_subscription_id: subscription_id,
            webhook_event_type: event_type,
            webhook_payload: content.payload,
            webhook_watermark: content.watermark,
        }
        .into_logical_plan())
    }

    #[tracing::instrument(name = "FlowControllerWebhookDeliver::propagate_success", skip_all)]
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
