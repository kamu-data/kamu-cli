// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use kamu_resources::{
    ApplyResourceCrudDispatcherError,
    ApplyResourceOutcome,
    ResourceCrudDispatcher,
    ResourceCrudDispatcherApplyRequest,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::use_cases::ResourceUseCaseBaseHarness;
use crate::tests::utils::{TestResource, make_account_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests — spec decoding / validation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_rejects_invalid_json_shape() {
    // Missing required field "value" — serde_json deserialization fails.
    let harness = ResourceCrudDispatcherHarness::new();

    let request = ResourceCrudDispatcherHarness::make_apply_request_with_spec(
        "res-a",
        serde_json::json!({ "unexpected_field": 42 }),
    );

    let err = harness.dispatcher().apply(request).await.unwrap_err();

    assert_matches!(
        err,
        ApplyResourceCrudDispatcherError::InvalidSpec {
            ref schema,
            ..
        } if schema == TestResource::SCHEMA,
        "expected InvalidSpec error, got: {err:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_rejects_semantically_invalid_spec() {
    // Empty value deserializes correctly but fails MockResourceSpec::validate()
    // inside the lifecycle, so the dispatcher returns
    // Ok(Rejected(BusinessValidationFailed)).
    let harness = ResourceCrudDispatcherHarness::new();

    let request = ResourceCrudDispatcherHarness::make_apply_request_with_spec(
        "res-a",
        serde_json::json!({ "value": "" }),
    );

    let decision = harness.dispatcher().apply(request).await.unwrap();

    let kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection) = decision else {
        panic!("expected Rejected decision, got: {decision:?}");
    };
    assert_eq!(
        rejection.category,
        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed
    );
    assert!(
        rejection.message.contains("value must not be empty"),
        "rejection message should mention the validation rule; got: {}",
        rejection.message
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_apply_rejects_semantically_invalid_spec() {
    // Empty value deserializes correctly but fails MockResourceSpec::validate()
    // inside the lifecycle, so the dispatcher returns
    // Ok(Rejected(BusinessValidationFailed)).
    let harness = ResourceCrudDispatcherHarness::new();

    let request = ResourceCrudDispatcherHarness::make_apply_request_with_spec(
        "res-a",
        serde_json::json!({ "value": "" }),
    );

    let decision = harness.dispatcher().plan_apply(request).await.unwrap();

    assert!(
        matches!(
            decision,
            kamu_resources::ApplyManifestPlanningDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            )
        ),
        "expected Rejected(BusinessValidationFailed), got: {decision:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_emits_lint_warning_for_long_value() {
    let harness = ResourceCrudDispatcherHarness::new();

    let long_value = "x".repeat(65);
    let request = ResourceCrudDispatcherHarness::make_apply_request("res-a", &long_value);

    let decision = harness.dispatcher().apply(request).await.unwrap();

    let applied = decision.expect_applied();
    assert_eq!(applied.warnings.len(), 1);
    assert_eq!(applied.warnings[0].code, "value_too_long");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_apply_emits_lint_warning_for_long_value() {
    let harness = ResourceCrudDispatcherHarness::new();

    let long_value = "x".repeat(65);
    let request = ResourceCrudDispatcherHarness::make_apply_request("res-a", &long_value);

    let decision = harness.dispatcher().plan_apply(request).await.unwrap();

    let plan = decision.expect_planned();
    assert_eq!(plan.warnings.len(), 1);
    assert_eq!(plan.warnings[0].code, "value_too_long");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests — orchestration / forwarding
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_creates_resource_and_returns_view() {
    let harness = ResourceCrudDispatcherHarness::new();

    let request = ResourceCrudDispatcherHarness::make_apply_request("res-a", "hello");

    let decision = harness.dispatcher().apply(request).await.unwrap();

    let applied = decision.expect_applied();

    assert_eq!(applied.outcome, ApplyResourceOutcome::Created);
    assert_eq!(applied.resource.schema, TestResource::SCHEMA);
    assert_eq!(
        applied.resource.spec,
        serde_json::json!({ "value": "hello" })
    );
    assert!(applied.warnings.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_apply_returns_planned_create() {
    let harness = ResourceCrudDispatcherHarness::new();

    let request = ResourceCrudDispatcherHarness::make_apply_request("res-a", "hello");

    let decision = harness.dispatcher().plan_apply(request).await.unwrap();

    let plan = decision.expect_planned();

    assert_eq!(plan.outcome, ApplyResourceOutcome::Created);
    // Generation change is always included in the diff for a create
    assert!(
        !plan.changes.is_empty(),
        "create plan must have at least the generation change"
    );
    assert!(plan.warnings.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_update_returns_applied_updated() {
    let harness = ResourceCrudDispatcherHarness::new();

    // First apply — Create
    let req1 = ResourceCrudDispatcherHarness::make_apply_request("res-a", "value-v1");
    let decision1 = harness.dispatcher().apply(req1).await.unwrap();
    let applied1 = decision1.expect_applied();
    assert_eq!(applied1.outcome, ApplyResourceOutcome::Created);
    let id = applied1.resource.headers.id;

    // Second apply with same account but different spec value — Update
    let account_id = applied1.resource.account.id.clone();
    let req2 = ResourceCrudDispatcherApplyRequest {
        id: Some(id),
        headers: BaseResourceServiceHarness::make_headers_input(account_id, "res-a"),
        spec: serde_json::json!({ "value": "value-v2" }),
    };
    let decision2 = harness.dispatcher().apply(req2).await.unwrap();
    let applied2 = decision2.expect_applied();
    assert_eq!(applied2.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(
        applied2.resource.spec,
        serde_json::json!({ "value": "value-v2" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_untouched_returns_applied_untouched() {
    let harness = ResourceCrudDispatcherHarness::new();

    // First apply
    let req1 = ResourceCrudDispatcherHarness::make_apply_request("res-a", "same-value");
    let decision1 = harness.dispatcher().apply(req1).await.unwrap();
    let applied1 = decision1.expect_applied();
    let id = applied1.resource.headers.id;
    let account_id = applied1.resource.account.id.clone();

    // Second apply — identical spec and headers
    let req2 = ResourceCrudDispatcherApplyRequest {
        id: Some(id),
        headers: BaseResourceServiceHarness::make_headers_input(account_id, "res-a"),
        spec: serde_json::json!({ "value": "same-value" }),
    };
    let decision2 = harness.dispatcher().apply(req2).await.unwrap();
    let applied2 = decision2.expect_applied();
    assert_eq!(applied2.outcome, ApplyResourceOutcome::Untouched);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ResourceUseCaseBaseHarness, base)]
struct ResourceCrudDispatcherHarness {
    base: ResourceUseCaseBaseHarness,
}

impl ResourceCrudDispatcherHarness {
    fn new() -> Self {
        Self {
            base: ResourceUseCaseBaseHarness::new(),
        }
    }

    fn dispatcher(&self) -> Arc<dyn ResourceCrudDispatcher> {
        self.catalog().get_one().unwrap()
    }

    fn make_apply_request(name: &str, value: &str) -> ResourceCrudDispatcherApplyRequest {
        let account_id = make_account_id();
        ResourceCrudDispatcherApplyRequest {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, name),
            spec: serde_json::json!({ "value": value }),
        }
    }

    fn make_apply_request_with_spec(
        name: &str,
        spec: serde_json::Value,
    ) -> ResourceCrudDispatcherApplyRequest {
        let account_id = make_account_id();
        ResourceCrudDispatcherApplyRequest {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, name),
            spec,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
