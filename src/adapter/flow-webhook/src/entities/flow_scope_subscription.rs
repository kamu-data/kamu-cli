// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::FLOW_SCOPE_ATTRIBUTE_DATASET_ID;
use kamu_flow_system as fs;
use kamu_webhooks::WebhookSubscriptionID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowScopeSubscription<'a>(&'a fs::FlowScope);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION: &str = "WebhookSubscription";

pub const FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID: &str = "subscription_id";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> FlowScopeSubscription<'a> {
    pub fn new(scope: &'a fs::FlowScope) -> Self {
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,);
        FlowScopeSubscription(scope)
    }

    pub fn make_scope(
        subscription_id: WebhookSubscriptionID,
        dataset_id: Option<&odf::DatasetID>,
    ) -> fs::FlowScope {
        let mut payload = serde_json::json!({
            fs::FLOW_SCOPE_ATTRIBUTE_TYPE: FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,
            FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID: subscription_id.to_string(),
        });

        if let Some(dataset_id) = dataset_id {
            payload[FLOW_SCOPE_ATTRIBUTE_DATASET_ID] =
                serde_json::Value::String(dataset_id.to_string());
        }

        fs::FlowScope::new(payload)
    }

    pub fn webhook_subscription_id(&self) -> WebhookSubscriptionID {
        self.0
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID)
            .and_then(|value| value.as_str())
            .and_then(|id_str| uuid::Uuid::parse_str(id_str).ok())
            .map(WebhookSubscriptionID::new)
            .unwrap_or_else(|| {
                panic!(
                    "FlowScopeSubscription must have a '{FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID}' \
                     attribute",
                )
            })
    }

    pub fn dataset_id(&self) -> Option<odf::DatasetID> {
        self.0
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_DATASET_ID)
            .and_then(|value| value.as_str())
            .map(|id_str| odf::DatasetID::from_did_str(id_str).expect("Invalid dataset ID format"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
