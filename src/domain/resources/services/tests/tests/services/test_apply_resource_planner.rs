// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::CatalogBuilder;
use kamu_resources::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourcePlan,
    ApplyResourcePlanningDecision,
    ApplyResourceUseCase,
    ApplyResourceUseCaseError,
    ResourceAggregateLoader,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceSnapshot,
    TypedResourceQueryService,
};
use kamu_resources_services::ApplyResourcePlanner;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    TestResource,
    TestResourceReconciler,
    TestResourceSpec,
    register_test_resource_resource_service_layer,
};
use odf::metadata::resources::TypeUri;

use crate::tests::utils::{ScriptedNameSuffixGenerator, make_id, make_resource_params};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_create_new_resource() {
    let harness = ApplyResourcePlannerHarness::new();
    let params = make_resource_params(harness.account_handle.clone(), "res-a");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Create
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_with_unknown_id_returns_not_found() {
    let harness = ApplyResourcePlannerHarness::new();
    let id = make_id();

    let params = harness.make_apply_params(Some(id), "res-a", "res-a");

    let result = harness.plan_result(params).await;

    pretty_assertions::assert_matches!(
        result,
        Err(ApplyResourceUseCaseError::ResourceIDNotFound(err)) if err.0 == id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_update_with_spec_change() {
    let harness = ApplyResourcePlannerHarness::new();

    let id = harness.apply_and_get_id("res-a").await;

    let params = harness.make_apply_params(Some(id), "res-a", "changed-value");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Update
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_untouched_no_changes() {
    let harness = ApplyResourcePlannerHarness::new();

    let id = harness.apply_and_get_id("res-a").await;

    let params = harness.make_apply_params(Some(id), "res-a", "res-a");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Untouched
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_update_via_name_lookup() {
    let harness = ApplyResourcePlannerHarness::new();

    harness.apply_and_get_id("res-a").await;

    // Plan with same name but no UID — planner resolves via name lookup
    let params = harness.make_apply_params(None, "res-a", "new-value");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Update
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_with_unknown_id_returns_not_found_even_if_name_exists() {
    let harness = ApplyResourcePlannerHarness::new();

    harness.apply_and_get_id("res-a").await;

    let stale_id = make_id();
    let params = harness.make_apply_params(Some(stale_id), "res-a", "new-value");

    let result = harness.plan_result(params).await;

    pretty_assertions::assert_matches!(
        result,
        Err(ApplyResourceUseCaseError::ResourceIDNotFound(err)) if err.0 == stale_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_type_mismatch_rejects() {
    let harness = ApplyResourcePlannerHarness::new();
    let id = harness.base.allocate_resource_id().await;

    // Insert a snapshot with the wrong schema directly into the repository
    harness
        .resource_repo()
        .create_resource(&ResourceSnapshot {
            id,
            schema: TypeUri::new_unchecked("OtherSchema"),
            headers: ResourceHeaders::simple(
                Utc::now(),
                id,
                harness.account_handle.clone(),
                "res-a",
            ),
            spec: serde_json::json!({}),
            status: None,
            last_event_id: None,
        })
        .await
        .unwrap();

    let params = harness.make_apply_params(Some(id), "res-a", "val");

    let result = harness.plan_result(params).await;

    assert!(
        result.is_err(),
        "expected type-mismatch error when UID points to a different schema"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_without_name_leaves_it_pending() {
    let harness = ApplyResourcePlannerHarness::new();

    for (value, placeholder) in [
        ("something", "plain-xxxxxx"),
        ("staged:initial", "staged-xxxxxx"),
    ] {
        let plan = harness.plan_nameless(None, value).await;

        assert_eq!(plan.action, ApplyResourceAction::Create);
        assert!(plan.name_pending);
        assert_eq!(plan.state.headers.name.as_str(), placeholder);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_with_id_and_without_name_keeps_the_name() {
    let harness = ApplyResourcePlannerHarness::new();
    let id = harness.apply_and_get_id("res-a").await;

    for (value, action) in [
        ("res-a", ApplyResourceAction::Untouched),
        ("changed", ApplyResourceAction::Update),
    ] {
        let plan = harness.plan_nameless(Some(id), value).await;

        assert_eq!(plan.id, id);
        assert_eq!(plan.action, action);
        assert!(!plan.name_pending);
        assert_eq!(plan.state.headers.name.as_str(), "res-a");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct ApplyResourcePlannerHarness {
    base: BaseResourceServiceHarness,
    _catalog: dill::Catalog,
    typed_query_svc: Arc<dyn TypedResourceQueryService<TestResource>>,
    aggregate_loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    apply_svc: Arc<dyn ApplyResourceUseCase<TestResource>>,
    // Planning without applying generates no names
    name_suffix_generator: ScriptedNameSuffixGenerator,
    account_handle: odf::ResourceOwnerHandle,
}

impl ApplyResourcePlannerHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        let catalog = b.build();

        let typed_query_svc = catalog.get_one().unwrap();
        let aggregate_loader = catalog.get_one().unwrap();
        let apply_svc = catalog.get_one().unwrap();

        Self {
            base,
            _catalog: catalog,
            typed_query_svc,
            aggregate_loader,
            apply_svc,
            name_suffix_generator: ScriptedNameSuffixGenerator::default(),
            account_handle: odf::ResourceOwnerHandle::new_test("test"),
        }
    }

    fn make_planner(&self) -> ApplyResourcePlanner<'_, TestResource> {
        ApplyResourcePlanner::new(
            self.generic_query_svc(),
            self.typed_query_svc.as_ref(),
            self.aggregate_loader.as_ref(),
            &self.name_suffix_generator,
            self.time_source(),
        )
    }

    async fn plan(
        &self,
        params: ApplyResourceParams<TestResource>,
    ) -> ApplyResourcePlanningDecision<TestResource> {
        self.plan_result(params).await.unwrap()
    }

    async fn plan_result(
        &self,
        params: ApplyResourceParams<TestResource>,
    ) -> Result<ApplyResourcePlanningDecision<TestResource>, ApplyResourceUseCaseError<TestResource>>
    {
        let planner = self.make_planner();
        let maybe_existing = planner.find_existing_resource(&params).await?;
        planner.plan(params, maybe_existing).await
    }

    fn make_apply_params(
        &self,
        id: Option<ResourceID>,
        name: &str,
        value: impl Into<String>,
    ) -> ApplyResourceParams<TestResource> {
        ApplyResourceParams {
            id,
            headers: BaseResourceServiceHarness::make_headers_input(
                self.account_handle.clone(),
                name,
            )
            .into(),
            spec: TestResourceSpec {
                value: value.into(),
            },
            provenance: BaseResourceServiceHarness::make_provenance(),
        }
    }

    fn make_nameless_params(
        &self,
        id: Option<ResourceID>,
        value: &str,
    ) -> ApplyResourceParams<TestResource> {
        ApplyResourceParams {
            id,
            headers: BaseResourceServiceHarness::make_nameless_headers(self.account_handle.clone()),
            spec: TestResourceSpec {
                value: value.to_string(),
            },
            provenance: BaseResourceServiceHarness::make_provenance(),
        }
    }

    async fn plan_nameless(
        &self,
        id: Option<ResourceID>,
        value: &str,
    ) -> ApplyResourcePlan<TestResource> {
        match self.plan(self.make_nameless_params(id, value)).await {
            ApplyResourcePlanningDecision::Planned(plan) => plan,
            ApplyResourcePlanningDecision::Rejected(r) => panic!("plan rejected: {}", r.message),
        }
    }

    async fn apply_and_get_id(&self, name: &str) -> ResourceID {
        let params = make_resource_params(self.account_handle.clone(), name);
        match self.apply_svc.apply(params).await.unwrap() {
            ApplyResourceApplicationDecision::Applied(result) => result.id,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("apply rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
