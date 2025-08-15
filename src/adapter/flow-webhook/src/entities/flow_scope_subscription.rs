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
use kamu_webhooks::{WebhookEventType, WebhookSubscriptionID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowScopeSubscription<'a>(&'a fs::FlowScope);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION: &str = "WebhookSubscription";

pub const FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID: &str = "subscription_id";
pub const FLOW_SCOPE_ATTRIBUTE_EVENT_TYPE: &str = "event_type";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> FlowScopeSubscription<'a> {
    pub fn new(scope: &'a fs::FlowScope) -> Self {
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,);
        FlowScopeSubscription(scope)
    }

    pub fn make_scope(
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        dataset_id: Option<&odf::DatasetID>,
    ) -> fs::FlowScope {
        let mut payload = serde_json::json!({
            fs::FLOW_SCOPE_ATTRIBUTE_TYPE: FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,
            FLOW_SCOPE_ATTRIBUTE_SUBSCRIPTION_ID: subscription_id.to_string(),
            FLOW_SCOPE_ATTRIBUTE_EVENT_TYPE: event_type.to_string(),
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

    pub fn event_type(&self) -> WebhookEventType {
        self.0
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_EVENT_TYPE)
            .and_then(|value| value.as_str())
            .and_then(|event_type_str| WebhookEventType::try_new(event_type_str).ok())
            .unwrap_or_else(|| {
                panic!(
                    "FlowScopeSubscription must have a '{FLOW_SCOPE_ATTRIBUTE_EVENT_TYPE}' \
                     attribute",
                )
            })
    }

    pub fn maybe_dataset_id(&self) -> Option<odf::DatasetID> {
        self.0
            .get_attribute(FLOW_SCOPE_ATTRIBUTE_DATASET_ID)
            .and_then(|value| value.as_str())
            .map(|id_str| odf::DatasetID::from_did_str(id_str).expect("Invalid dataset ID format"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_adapter_flow_dataset::FlowScopeDataset;
    use kamu_webhooks::WebhookEventTypeCatalog;

    use super::*;

    #[test]
    fn test_scope_pack_unpack_without_dataset_id() {
        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
        let event_type = WebhookEventTypeCatalog::test();

        let scope = FlowScopeSubscription::make_scope(subscription_id, &event_type, None);
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION);

        let unpacked = FlowScopeSubscription::new(&scope);
        assert_eq!(unpacked.webhook_subscription_id(), subscription_id);
        assert_eq!(unpacked.event_type(), event_type);
        assert_eq!(unpacked.maybe_dataset_id(), None);
    }

    #[test]
    fn test_scope_pack_unpack_with_dataset_id() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"test_dataset");
        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
        let event_type = WebhookEventTypeCatalog::test();

        let scope =
            FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&dataset_id));
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION);

        let unpacked = FlowScopeSubscription::new(&scope);
        assert_eq!(unpacked.webhook_subscription_id(), subscription_id);
        assert_eq!(unpacked.event_type(), event_type);
        assert_eq!(unpacked.maybe_dataset_id(), Some(dataset_id));
    }

    #[test]
    fn test_matches_single_dataset_query() {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"test_dataset");
        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
        let event_type = WebhookEventTypeCatalog::test();

        let scope1 =
            FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&dataset_id));
        let scope2 = FlowScopeSubscription::make_scope(subscription_id, &event_type, None);

        let query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
        assert!(scope1.matches_query(&query));
        assert!(!scope2.matches_query(&query));

        let wrong_query = FlowScopeDataset::query_for_single_dataset(
            &odf::DatasetID::new_seeded_ed25519(b"wrong_dataset"),
        );
        assert!(!scope1.matches_query(&wrong_query));
        assert!(!scope2.matches_query(&wrong_query));
    }

    #[test]
    fn test_matches_multiple_datasets_query() {
        let dataset_id1 = odf::DatasetID::new_seeded_ed25519(b"test_dataset1");
        let dataset_id2 = odf::DatasetID::new_seeded_ed25519(b"test_dataset2");
        let dataset_id3 = odf::DatasetID::new_seeded_ed25519(b"test_dataset3");
        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
        let event_type = WebhookEventTypeCatalog::test();

        let scope1 =
            FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&dataset_id1));
        let scope2 =
            FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&dataset_id2));
        let scope3 = FlowScopeSubscription::make_scope(subscription_id, &event_type, None);

        let query = FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id1, &dataset_id2]);
        assert!(scope1.matches_query(&query));
        assert!(scope2.matches_query(&query));
        assert!(!scope3.matches_query(&query));

        let partially_wrong_query =
            FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id1, &dataset_id3]);
        assert!(scope1.matches_query(&partially_wrong_query));
        assert!(!scope2.matches_query(&partially_wrong_query));
        assert!(!scope3.matches_query(&partially_wrong_query));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
