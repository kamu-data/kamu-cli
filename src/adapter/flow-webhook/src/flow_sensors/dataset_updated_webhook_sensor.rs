// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, DatasetResourceUpdateDetails};
use kamu_adapter_task_webhook::TaskRunArgumentsWebhookDeliver;
use kamu_flow_system as fs;
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookPayloadBuilder};

use crate::FLOW_TYPE_WEBHOOK_DELIVER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetUpdatedWebhookSensor {
    webhook_flow_scope: fs::FlowScope,
    batching_rule: fs::BatchingRule,
}

impl DatasetUpdatedWebhookSensor {
    pub fn new(webhook_flow_scope: fs::FlowScope, batching_rule: fs::BatchingRule) -> Self {
        Self {
            webhook_flow_scope,
            batching_rule,
        }
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DatasetUpdatedWebhookSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.webhook_flow_scope
    }

    fn get_sensitive_to_scopes(&self) -> Vec<fs::FlowScope> {
        vec![fs::FlowScope::for_dataset(
            self.webhook_flow_scope.dataset_id().unwrap(),
        )]
    }

    async fn on_activated(
        &self,
        _catalog: &dill::Catalog,
        _activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.webhook_flow_scope, "DatasetUpdatedWebhookSensor activated");
        // TODO
        Ok(())
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.webhook_flow_scope, ?input_flow_binding, ?activation_cause, "DatasetUpdatedWebhookSensor sensitized");

        // Ensure sensitized for right dataset id
        let input_dataset_id = input_flow_binding.scope.dataset_id().ok_or_else(|| {
            InternalError::new("Input flow binding does not have a dataset ID".to_string())
        })?;
        if input_dataset_id != self.webhook_flow_scope.dataset_id().unwrap() {
            return Err(InternalError::new(format!(
                "FlowBinding dataset ID {} does not match sensor dataset ID {}",
                input_dataset_id,
                self.webhook_flow_scope.dataset_id().unwrap()
            )));
        }

        // React to dataset updates
        if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
            // Decode dataset update
            if update.resource_type != DATASET_RESOURCE_TYPE {
                return Err(InternalError::new(format!(
                    "Unexpected resource type: {}",
                    update.resource_type
                )));
            }
            let dataset_update_details: DatasetResourceUpdateDetails =
                serde_json::from_value(update.details.clone()).int_err()?;

            // Extract necessary services
            let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();
            let webhook_payload_builder = catalog.get_one::<dyn WebhookPayloadBuilder>().unwrap();

            // Build webhook payload
            let webhook_payload = webhook_payload_builder
                .build_dataset_ref_updated_payload(
                    input_dataset_id,
                    &odf::BlockRef::Head,
                    &dataset_update_details.new_head,
                    dataset_update_details.old_head_maybe.as_ref(),
                )
                .await
                .int_err()?;

            // Trigger webhook flow, regardless of the change type
            let target_flow_binding =
                fs::FlowBinding::new(FLOW_TYPE_WEBHOOK_DELIVER, self.webhook_flow_scope.clone());
            flow_run_service
                .run_flow_automatically(
                    &target_flow_binding,
                    activation_cause.clone(),
                    Some(fs::FlowTriggerRule::Batching(self.batching_rule)),
                    None,
                    Some(
                        TaskRunArgumentsWebhookDeliver {
                            event_type: WebhookEventTypeCatalog::dataset_ref_updated(),
                            payload: webhook_payload,
                        }
                        .into_task_run_arguments(),
                    ),
                )
                .await
                .int_err()?;

            Ok(())
        } else {
            Err(InternalError::new(format!(
                "Invalid activation cause for DatasetUpdatedWebhookSensor: {activation_cause:?}",
            )))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
