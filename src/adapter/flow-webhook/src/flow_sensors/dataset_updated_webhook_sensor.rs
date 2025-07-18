// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_task_webhook::TaskRunArgumentsWebhookDeliver;
use kamu_flow_system as fs;
use kamu_webhooks::{
    InternalError,
    ResultIntoInternal,
    WebhookEventTypeCatalog,
    WebhookPayloadBuilder,
    WebhookSubscription,
};

use crate::FLOW_TYPE_WEBHOOK_DELIVER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetUpdatedWebhookSensor {
    flow_scope: fs::FlowScope,
}

impl DatasetUpdatedWebhookSensor {
    pub fn new(webhook_subscription: &WebhookSubscription) -> Self {
        Self {
            flow_scope: fs::FlowScope::WebhookSubscription {
                subscription_id: webhook_subscription.id().into_inner(),
                dataset_id: webhook_subscription.dataset_id().cloned(),
            },
        }
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DatasetUpdatedWebhookSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.flow_scope
    }

    fn get_sensitive_datasets(&self) -> Vec<odf::DatasetID> {
        vec![self.flow_scope.dataset_id().unwrap().clone()]
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), InternalError> {
        // Ensure sensitized for right dataset id
        let input_dataset_id = input_flow_binding.dataset_id().ok_or_else(|| {
            InternalError::new("Input flow binding does not have a dataset ID".to_string())
        })?;
        if input_dataset_id != self.flow_scope.dataset_id().unwrap() {
            return Err(InternalError::new(format!(
                "FlowBinding dataset ID {} does not match sensor dataset ID {}",
                input_dataset_id,
                self.flow_scope.dataset_id().unwrap()
            )));
        }

        // React to dataset updates
        if let fs::FlowActivationCause::DatasetUpdate(update) = activation_cause {
            // Extract necessary services
            let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();
            let webhook_payload_builder = catalog.get_one::<dyn WebhookPayloadBuilder>().unwrap();

            // Build webhook payload
            let webhook_payload = webhook_payload_builder
                .build_dataset_ref_updated_payload(
                    input_dataset_id,
                    &odf::BlockRef::Head,
                    &update.new_head,
                    update.old_head_maybe.as_ref(),
                )
                .await
                .int_err()?;

            // Trigger webhook flow, regardless of the change type
            let target_flow_binding =
                fs::FlowBinding::from_scope(FLOW_TYPE_WEBHOOK_DELIVER, self.flow_scope.clone());
            flow_run_service
                .run_flow_automatically(
                    &target_flow_binding,
                    activation_cause.clone(),
                    None,
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
