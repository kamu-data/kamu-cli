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

use chrono::Utc;
use dill::CatalogBuilder;
use kamu_resources::{
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourcePrecondition,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    ManagedResource,
    ResourceAggregateLoader,
    ResourceLifecycleMessageOutcome,
    ResourcePersistenceService,
    TypedResourceQueryService,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
    TestResource,
    TestResourceReconciler,
    TestResourceSpec,
    register_test_resource_resource_service_layer,
};
use kamu_resources_services::{
    ApplyResourceExecution,
    ApplyResourcePlanExecutor,
    ApplyResourcePlanner,
    PlannedApplyResource,
    PlannedApplyResourceDecision,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use crate::tests::utils::{ScriptedNameSuffixGenerator, make_fresh_aggregate};

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
    let account_handle = odf::ResourceOwnerHandle::new_test("test");

    let plan = harness
        .make_plan(
            account_handle,
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;

    let result = harness.execute(plan).await;

    assert_eq!(result.outcome, ApplyResourceOutcome::Created);
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
    let account_handle = odf::ResourceOwnerHandle::new_test("test");

    // Create first
    let create_plan = harness
        .make_plan(
            account_handle.clone(),
            "res-a",
            TestResourceSpec {
                value: "initial".to_string(),
            },
        )
        .await;
    harness.execute(create_plan).await;

    // Then update
    let update_plan = harness
        .make_plan(
            account_handle,
            "res-a",
            TestResourceSpec {
                value: "updated".to_string(),
            },
        )
        .await;

    let result = harness.execute(update_plan).await;

    assert_eq!(result.outcome, ApplyResourceOutcome::Updated);
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
    let account_handle = odf::ResourceOwnerHandle::new_test("test");

    // Create the resource first (sends one outbox message)
    let create_plan = harness
        .make_plan(
            account_handle.clone(),
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;
    harness.execute(create_plan).await;

    // Re-plan with identical spec — action = Untouched, no outbox call
    let untouched_plan = harness
        .make_plan(
            account_handle,
            "res-a",
            TestResourceSpec {
                value: "res-a".to_string(),
            },
        )
        .await;

    let result = harness.execute(untouched_plan).await;

    assert_eq!(result.outcome, ApplyResourceOutcome::Untouched);

    // MockOutbox verifies at drop that post_message_as_json was called exactly
    // once (from create)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_create_reports_a_taken_name() {
    // No message expected: nothing is written
    let harness = ApplyResourcePlanExecutorHarness::new(MockOutbox::new());

    // Another writer creates the resource after the plan was made
    let plan = harness
        .make_plan(
            ApplyResourcePlanExecutorHarness::account_handle(),
            "res-a",
            TestResourceSpec {
                value: "new-value".to_string(),
            },
        )
        .await;
    let winner = harness.store("res-a").await;

    assert_matches!(
        harness.make_executor().execute(plan).await,
        Ok(ApplyResourceExecution::NameTaken)
    );
    harness.assert_untouched(&winner).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_generated_name_avoids_names_of_resources_in_any_state() {
    let harness = ApplyResourcePlanExecutorHarness::with_name_suffixes(
        ApplyResourcePlanExecutorHarness::mock_outbox_for_create(),
        ["live00", "dying0", "dead00", "free00"],
    );
    harness.store("plain-live00").await;
    harness.store_being_deleted("plain-dying0").await;
    harness.store_deleted("plain-dead00").await;

    let plan = harness.make_nameless_plan().await;
    let result = harness.execute(plan).await;

    assert_eq!(result.outcome, ApplyResourceOutcome::Created);
    assert_eq!(result.state.headers.name.as_str(), "plain-free00");
    harness.name_suffix_generator.assert_all_used();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_generated_name_taken_after_planning_is_replaced() {
    let harness = ApplyResourcePlanExecutorHarness::with_name_suffixes(
        ApplyResourcePlanExecutorHarness::mock_outbox_for_create(),
        ["first0", "second"],
    );
    let plan = harness.make_nameless_plan().await;
    let winner = harness.store("plain-first0").await;

    let result = harness.execute(plan).await;

    // A resource of its own, rather than an update of the winner
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);
    assert_eq!(result.state.headers.name.as_str(), "plain-second");
    harness.assert_untouched(&winner).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_generating_a_name_gives_up_after_five_attempts() {
    // While planning
    let harness =
        ApplyResourcePlanExecutorHarness::with_name_suffixes(MockOutbox::new(), ["taken0"; 5]);
    harness.store("plain-taken0").await;
    assert_matches!(
        harness.plan_nameless().await.err(),
        Some(ApplyResourceUseCaseError::Internal(_))
    );
    harness.name_suffix_generator.assert_all_used();

    // Attempts of planning count while applying
    let harness = ApplyResourcePlanExecutorHarness::with_name_suffixes(
        MockOutbox::new(),
        ["taken0", "taken0", "lost00", "taken0", "taken0"],
    );
    harness.store("plain-taken0").await;
    let plan = harness.make_nameless_plan().await;
    harness.store("plain-lost00").await;
    assert_matches!(
        harness.make_executor().execute(plan).await,
        Err(ApplyResourceUseCaseError::Internal(_))
    );
    harness.name_suffix_generator.assert_all_used();
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
    name_suffix_generator: ScriptedNameSuffixGenerator,
}

impl ApplyResourcePlanExecutorHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base = BaseResourceServiceHarness::new_with_opts(BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Mock(mock_outbox),
            ..Default::default()
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
            name_suffix_generator: ScriptedNameSuffixGenerator::default(),
        }
    }

    fn with_name_suffixes(
        mock_outbox: MockOutbox,
        suffixes: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        Self {
            name_suffix_generator: ScriptedNameSuffixGenerator::new(suffixes),
            ..Self::new(mock_outbox)
        }
    }

    /// Expects the applied message of one created resource
    fn mock_outbox_for_create() -> MockOutbox {
        let mut mock_outbox = MockOutbox::new();
        BaseResourceServiceHarness::expect_applied_messages(
            &mut mock_outbox,
            1,
            Some(ResourceLifecycleMessageOutcome::Created),
        );
        mock_outbox
    }

    fn account_handle() -> odf::ResourceOwnerHandle {
        odf::ResourceOwnerHandle::new_test("test")
    }

    /// Stores a resource as another writer would, without a message
    async fn store(&self, name: &str) -> TestResource {
        let (_, mut resource) = make_fresh_aggregate(Self::account_handle(), name);
        self.persistence_svc.create(&mut resource).await.unwrap();
        resource
    }

    async fn store_being_deleted(&self, name: &str) -> TestResource {
        let mut resource = self.store(name).await;
        resource.try_request_deletion(Utc::now()).unwrap();
        self.persistence_svc.save(&mut resource).await.unwrap();
        resource
    }

    async fn store_deleted(&self, name: &str) {
        let mut resource = self.store_being_deleted(name).await;
        resource.try_complete_deletion(Utc::now()).unwrap();
        self.persistence_svc.save(&mut resource).await.unwrap();
    }

    async fn plan_nameless(
        &self,
    ) -> Result<PlannedApplyResourceDecision<TestResource>, ApplyResourceUseCaseError<TestResource>>
    {
        self.plan(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_nameless_headers(Self::account_handle()),
            spec: TestResourceSpec {
                value: "nameless".to_string(),
            },
        })
        .await
    }

    async fn plan(
        &self,
        params: ApplyResourceParams<TestResource>,
    ) -> Result<PlannedApplyResourceDecision<TestResource>, ApplyResourceUseCaseError<TestResource>>
    {
        let planner = self.make_planner();
        let maybe_existing = planner.find_existing_resource(&params).await?;
        planner
            .plan_internal(params, maybe_existing, ApplyResourcePrecondition::None)
            .await
    }

    async fn make_nameless_plan(&self) -> PlannedApplyResource<TestResource> {
        match self.plan_nameless().await.unwrap() {
            PlannedApplyResourceDecision::Planned(plan) => plan,
            PlannedApplyResourceDecision::Rejected(r) => panic!("plan rejected: {}", r.message),
        }
    }

    async fn execute(
        &self,
        plan: PlannedApplyResource<TestResource>,
    ) -> ApplyResourceResult<TestResource> {
        match self.make_executor().execute(plan).await.unwrap() {
            ApplyResourceExecution::Decided(decision) => decision.expect_applied(),
            ApplyResourceExecution::NameTaken => panic!("name taken"),
        }
    }

    async fn assert_untouched(&self, resource: &TestResource) {
        let stored = self.aggregate_loader.load(resource.id()).await.unwrap();
        assert_eq!(stored.spec(), resource.spec());
        assert_eq!(stored.headers().generation, 1);
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

    fn make_executor(&self) -> ApplyResourcePlanExecutor<'_, TestResource> {
        ApplyResourcePlanExecutor::new(
            self.generic_query_svc(),
            self.typed_query_svc.as_ref(),
            self.aggregate_loader.as_ref(),
            self.persistence_svc.as_ref(),
            &self.name_suffix_generator,
            self.outbox(),
            self.time_source(),
        )
    }

    async fn make_plan(
        &self,
        account: odf::ResourceOwnerHandle,
        name: &str,
        spec: TestResourceSpec,
    ) -> PlannedApplyResource<TestResource> {
        let params = ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account, name).into(),
            spec,
        };

        match self.plan(params).await.unwrap() {
            PlannedApplyResourceDecision::Planned(plan) => plan,
            PlannedApplyResourceDecision::Rejected(r) => {
                panic!("plan rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
