// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_resources::{
    ApplyResourceOutcome,
    ApplyResourceParams,
    ResourceAggregateLoader,
    ResourceLifecycleMessageOutcome,
    ResourcePersistenceService,
    TypedResourceQueryService,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use kamu_resources_services::{
    ApplyResourcePlanExecutor,
    ApplyResourcePlanner,
    PlannedApplyResource,
    PlannedApplyResourceDecision,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use crate::tests::utils::{
    TestResource,
    TestResourceReconciler,
    TestResourceSpec,
    make_account_id,
    make_fresh_aggregate,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_create_returns_applied_created() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(
        &mut mock_outbox,
        1,
        Some(ResourceLifecycleMessageOutcome::Created),
    );

    let harness = ApplyResourcePlanExecutorHarness::new(mock_outbox);
    let account_id = make_account_id();

    let plan = harness
        .make_plan(
            account_id,
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;

    let result = harness.make_executor().execute(plan).await.unwrap();

    let applied = result.expect_applied();
    assert_eq!(applied.outcome, ApplyResourceOutcome::Created);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_update_returns_applied_updated() {
    let mut mock_outbox = MockOutbox::new();
    // One message for the initial create, one for the update
    BaseResourceServiceHarness::expect_applied_messages(
        &mut mock_outbox,
        1,
        Some(ResourceLifecycleMessageOutcome::Created),
    );
    BaseResourceServiceHarness::expect_applied_messages(
        &mut mock_outbox,
        1,
        Some(ResourceLifecycleMessageOutcome::Updated),
    );

    let harness = ApplyResourcePlanExecutorHarness::new(mock_outbox);
    let account_id = make_account_id();

    // Create first
    let create_plan = harness
        .make_plan(
            account_id.clone(),
            "res-a",
            TestResourceSpec {
                value: "initial".to_string(),
            },
        )
        .await;
    harness.make_executor().execute(create_plan).await.unwrap();

    // Then update
    let update_plan = harness
        .make_plan(
            account_id,
            "res-a",
            TestResourceSpec {
                value: "updated".to_string(),
            },
        )
        .await;

    let result = harness.make_executor().execute(update_plan).await.unwrap();

    let applied = result.expect_applied();
    assert_eq!(applied.outcome, ApplyResourceOutcome::Updated);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_untouched_returns_applied_no_outbox_call() {
    let mut mock_outbox = MockOutbox::new();
    // Exactly one outbox call: for the initial create. None for the untouched
    // re-plan.
    BaseResourceServiceHarness::expect_applied_messages(
        &mut mock_outbox,
        1,
        Some(ResourceLifecycleMessageOutcome::Created),
    );

    let harness = ApplyResourcePlanExecutorHarness::new(mock_outbox);
    let account_id = make_account_id();

    // Create the resource first (sends one outbox message)
    let create_plan = harness
        .make_plan(
            account_id.clone(),
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;
    harness.make_executor().execute(create_plan).await.unwrap();

    // Re-plan with identical spec — action = Untouched, no outbox call
    let untouched_plan = harness
        .make_plan(
            account_id,
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;

    let result = harness
        .make_executor()
        .execute(untouched_plan)
        .await
        .unwrap();

    let applied = result.expect_applied();
    assert_eq!(applied.outcome, ApplyResourceOutcome::Untouched);

    // MockOutbox verifies at drop that post_message_as_json was called exactly
    // once (from create)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_create_duplicate_retries_as_update() {
    // One message from the retry-as-update path (the initial direct persist
    // goes straight to the store, not through the executor, so no create msg)
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(
        &mut mock_outbox,
        1,
        Some(ResourceLifecycleMessageOutcome::Updated),
    );

    let harness = ApplyResourcePlanExecutorHarness::new(mock_outbox);
    let account_id = make_account_id();

    // Persist the resource directly so it already exists in the store —
    // bypassing the outbox path so no message is expected for this step.
    let (_, mut existing_agg) = make_fresh_aggregate(account_id.clone(), "res-a");
    harness
        .persistence_svc
        .create(&mut existing_agg)
        .await
        .unwrap();

    // Plan a Create with id=None and a fresh name (no prior UID hint).
    // plan_create_resource allocates a new UID without doing a name lookup,
    // so the plan carries action=Create. When the executor calls
    // persistence_svc.create(), the store rejects it with Duplicate (same
    // account + kind + name already exists). The executor then retries via
    // the update path, which resolves the conflict by name lookup.
    let create_plan = harness
        .make_plan(
            account_id,
            "res-a",
            TestResourceSpec {
                value: "new-value".to_string(),
            },
        )
        .await;

    let result = harness.make_executor().execute(create_plan).await.unwrap();

    let applied = result.expect_applied();
    assert_eq!(applied.outcome, ApplyResourceOutcome::Updated);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct ApplyResourcePlanExecutorHarness {
    base: BaseResourceServiceHarness,
    _catalog: dill::Catalog,
    typed_query_svc: Arc<dyn TypedResourceQueryService<TestResource>>,
    aggregate_loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    persistence_svc: Arc<dyn ResourcePersistenceService<TestResource>>,
}

impl ApplyResourcePlanExecutorHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base = BaseResourceServiceHarness::new_with_opts(BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Mock(mock_outbox),
        });

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        let catalog = b.build();

        let typed_query_svc = catalog.get_one().unwrap();
        let aggregate_loader = catalog.get_one().unwrap();
        let persistence_svc = catalog.get_one().unwrap();

        Self {
            base,
            _catalog: catalog,
            typed_query_svc,
            aggregate_loader,
            persistence_svc,
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

    fn make_executor(&self) -> ApplyResourcePlanExecutor<'_, TestResource> {
        ApplyResourcePlanExecutor::new(
            self.generic_query_svc(),
            self.typed_query_svc.as_ref(),
            self.aggregate_loader.as_ref(),
            self.persistence_svc.as_ref(),
            self.outbox(),
            self.time_source(),
        )
    }

    async fn make_plan(
        &self,
        account_id: odf::AccountID,
        name: &str,
        spec: TestResourceSpec,
    ) -> PlannedApplyResource<TestResource> {
        let params = ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, name),
            spec,
        };

        match self.make_planner().plan_internal(params).await.unwrap() {
            PlannedApplyResourceDecision::Planned(plan) => plan,
            PlannedApplyResourceDecision::Rejected(r) => {
                panic!("plan rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
