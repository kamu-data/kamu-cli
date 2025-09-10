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
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    make_dataset_flow_sort_key,
};
use kamu_datasets::DatasetEntryService;
use kamu_webhooks::{
    ResultIntoInternal,
    WebhookEventType,
    WebhookEventTypeCatalog,
    WebhookSubscriptionQueryMode,
};
use {kamu_adapter_task_webhook as atw, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    DatasetUpdatedWebhookSensor,
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
    WEBHOOK_DATASET_REF_UPDATED_VERSION,
    WebhookDatasetRefUpdatedPayload,
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
    webhook_subscription_query_service: Arc<dyn kamu_webhooks::WebhookSubscriptionQueryService>,
}

impl FlowControllerWebhookDeliver {
    async fn build_payload(
        &self,
        event_type: &WebhookEventType,
        maybe_dataset_id: Option<&odf::DatasetID>,
        flow: &fs::FlowState,
    ) -> Result<serde_json::Value, InternalError> {
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
            panic!("FlowControllerWebhookDeliver does not support event type: {event_type}",);
        }
    }

    async fn build_dataset_ref_updated_payload(
        &self,
        flow: &fs::FlowState,
        input_dataset_id: &odf::DatasetID,
    ) -> Result<serde_json::Value, InternalError> {
        // Find out who is the owner of the dataset
        let dataset_entry = self
            .dataset_entry_service
            .get_entry(input_dataset_id)
            .await
            .int_err()?;

        // We need to build a summary of 1..N changes that happened to the input
        // dataset, so that webhook was activated
        struct ChangeSummary {
            new_head: odf::Multihash,
            old_head_maybe: Option<odf::Multihash>,
            is_breaking_change: bool,
        }
        let mut summary: Option<ChangeSummary> = None;

        // Scan activation causes for the flow, look for dataset updates only
        for activation_cause in &flow.activation_causes {
            if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
                assert_eq!(update.resource_type, DATASET_RESOURCE_TYPE);
                let dataset_update_details =
                    serde_json::from_value::<DatasetResourceUpdateDetails>(update.details.clone())
                        .int_err()?;

                assert_eq!(
                    &dataset_update_details.dataset_id, input_dataset_id,
                    "Dataset ID in update details does not match input dataset ID"
                );

                if let Some(summary) = &mut summary {
                    summary.new_head = dataset_update_details.new_head;
                    summary.is_breaking_change |=
                        matches!(update.changes, fs::ResourceChanges::Breaking);
                } else {
                    summary = Some(ChangeSummary {
                        new_head: dataset_update_details.new_head,
                        old_head_maybe: dataset_update_details.old_head_maybe,
                        is_breaking_change: matches!(update.changes, fs::ResourceChanges::Breaking),
                    });
                }
            }
        }
        let summary = summary.ok_or_else(|| {
            InternalError::new("No dataset updates found in the flow state for the input dataset")
        })?;

        // Build webhook payload
        let webhook_payload = serde_json::to_value(WebhookDatasetRefUpdatedPayload {
            version: WEBHOOK_DATASET_REF_UPDATED_VERSION,
            dataset_id: input_dataset_id.to_string(),
            owner_account_id: dataset_entry.owner_id.to_string(),
            block_ref: odf::BlockRef::Head.to_string(),
            new_hash: summary.new_head.to_string(),
            old_hash: summary.old_head_maybe.map(|h| h.to_string()),
            is_breaking_change: summary.is_breaking_change,
        })
        .int_err()?;

        Ok(webhook_payload)
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
        activation_time: DateTime<Utc>,
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
                .register_sensor(&self.catalog, activation_time, sensor)
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

        let webhook_payload = self
            .build_payload(&event_type, maybe_dataset_id.as_ref(), flow)
            .await?;

        Ok(atw::LogicalPlanWebhookDeliver {
            webhook_subscription_id: subscription_id,
            webhook_event_type: event_type,
            webhook_payload,
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

    async fn make_flow_sort_key(
        &self,
        flow_binding: &fs::FlowBinding,
    ) -> Result<String, InternalError> {
        let scope = FlowScopeSubscription::new(&flow_binding.scope);

        // Load webhook subscription
        let subscription_id = scope.subscription_id();
        let Some(webhook_subscription) = self
            .webhook_subscription_query_service
            .find_webhook_subscription(subscription_id, WebhookSubscriptionQueryMode::Active)
            .await
            .int_err()?
        else {
            return InternalError::bail(format!(
                "Failed to find webhook subscription with ID: {subscription_id}"
            ));
        };

        // Find out it's sort key suffix
        let webhook_sort_key_suffix = webhook_subscription.sort_key();

        // If dataset is involved, prepend it's sort key
        match scope.maybe_dataset_id() {
            None => Ok(webhook_sort_key_suffix),
            Some(dataset_id) => {
                let dataset_sort_key =
                    make_dataset_flow_sort_key(self.dataset_entry_service.as_ref(), &dataset_id)
                        .await?;

                Ok(format!("{dataset_sort_key}/{webhook_sort_key_suffix}"))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
