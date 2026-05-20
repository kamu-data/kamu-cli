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
    ApplyResourcePlanningDecision,
    ApplyResourceUseCase,
    ApplyResourceUseCaseError,
    ResourceAggregateLoader,
    ResourceMetadata,
    ResourceSnapshot,
    ResourceUID,
    TypedResourceQueryService,
};
use kamu_resources_services::ApplyResourcePlanner;
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{
    TestResource,
    TestResourceReconciler,
    TestResourceSpec,
    make_account_id,
    make_resource_params,
    make_uid,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_create_new_resource() {
    let harness = ApplyResourcePlannerHarness::new();
    let account_id = make_account_id();
    let params = make_resource_params(account_id, "res-a");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Create
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_create_with_uid_hint_not_yet_saved() {
    let harness = ApplyResourcePlannerHarness::new();
    let account_id = make_account_id();
    let uid = make_uid();

    let params = harness.make_apply_params(Some(uid), account_id, "res-a", "res-a");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Create
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_update_with_spec_change() {
    let harness = ApplyResourcePlannerHarness::new();
    let account_id = make_account_id();

    let uid = harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    let params = harness.make_apply_params(Some(uid), account_id.clone(), "res-a", "changed-value");

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
    let account_id = make_account_id();

    let uid = harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    let params = harness.make_apply_params(Some(uid), account_id.clone(), "res-a", "res-a");

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
    let account_id = make_account_id();

    harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    // Plan with same name but no UID — planner resolves via name lookup
    let params = harness.make_apply_params(None, account_id, "res-a", "new-value");

    let decision = harness.plan(params).await;

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(plan) if plan.action == ApplyResourceAction::Update
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_type_mismatch_rejects() {
    let harness = ApplyResourcePlannerHarness::new();
    let account_id = make_account_id();
    let uid = harness.base.allocate_resource_uid().await;

    // Insert a snapshot with the wrong kind directly into the repository
    harness
        .resource_repo()
        .create_resource(&ResourceSnapshot {
            uid,
            kind: "OtherKind".to_string(),
            api_version: "other.dev/v1".to_string(),
            metadata: ResourceMetadata::new_minimal(
                Utc::now(),
                account_id.clone(),
                "res_a".to_string(),
            ),
            spec: serde_json::json!({}),
            status: None,
            last_reconciled_at: None,
            last_event_id: None,
        })
        .await
        .unwrap();

    let params = harness.make_apply_params(Some(uid), account_id, "res-a", "val");

    let result = harness.plan_result(params).await;

    assert!(
        result.is_err(),
        "expected type-mismatch error when UID points to a different kind"
    );
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
        }
    }

    fn make_planner(&self) -> ApplyResourcePlanner<'_, TestResource> {
        ApplyResourcePlanner::new(
            self.generic_query_svc(),
            self.typed_query_svc.as_ref(),
            self.aggregate_loader.as_ref(),
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
        self.make_planner().plan(params).await
    }

    fn make_apply_params(
        &self,
        uid: Option<ResourceUID>,
        account_id: odf::AccountID,
        name: &str,
        value: impl Into<String>,
    ) -> ApplyResourceParams<TestResource> {
        ApplyResourceParams {
            uid,
            metadata: BaseResourceServiceHarness::make_metadata_input(account_id, name),
            spec: TestResourceSpec {
                value: value.into(),
            },
        }
    }

    async fn apply_and_get_uid(&self, account_id: odf::AccountID, name: &str) -> ResourceUID {
        let params = make_resource_params(account_id, name);
        match self.apply_svc.apply(params).await.unwrap() {
            ApplyResourceApplicationDecision::Applied(result) => result.uid,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("apply rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
