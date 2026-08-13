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
use database_common::PaginationOpts;
use dill::CatalogBuilder;
use kamu_resources::{
    ApplyResourceUseCase,
    DeclarativeResourceState,
    ResolvedResourceLabelFilter,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceSnapshot,
    TypeUri,
    TypedResourceQueryError,
    TypedResourceQueryService,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{
    TestResource,
    TestResourceReconciler,
    make_resource_params,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static OTHER_SCHEMA: std::sync::LazyLock<TypeUri> =
    std::sync::LazyLock::new(|| TypeUri::new_unchecked("OtherKind"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_state_by_id_success() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let state = harness
        .typed_query_svc()
        .get_state_by_id(account_handle.did, &id)
        .await
        .unwrap();

    assert_eq!(state.spec().value, "res-a");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_state_by_id_not_found() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");
    let id = harness.allocate_id().await;

    let result = harness
        .typed_query_svc()
        .get_state_by_id(account_handle.did, &id)
        .await;

    assert!(
        matches!(result, Err(TypedResourceQueryError::NotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_state_by_id_wrong_account() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle_a = odf::AccountHandle::new_test("test-owner-a");
    let account_handle_b = odf::AccountHandle::new_test("test-owner-b");
    let id = harness.apply_and_get_id(&account_handle_a, "res-a").await;

    let result = harness
        .typed_query_svc()
        .get_state_by_id(account_handle_b.did, &id)
        .await;

    // Wrong account: implementation returns NotFound to avoid information leakage
    assert!(
        matches!(result, Err(TypedResourceQueryError::NotFound(_))),
        "expected NotFound for wrong account, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_state_by_id_type_mismatch() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot_with_schema(id, &account_handle, &OTHER_SCHEMA, "res-a")
        .await;

    let result = harness
        .typed_query_svc()
        .get_state_by_id(account_handle.did, &id)
        .await;

    // get_state_by_id uses get_snapshot_by_query which filters by schema, so a
    // snapshot with a different schema is invisible to this query → NotFound, not
    // TypeMismatch. TypeMismatch is covered by ensure_resource_id_matches_type
    // which uses load_snapshot_by_id (schema-agnostic lookup).
    assert!(
        matches!(result, Err(TypedResourceQueryError::NotFound(_))),
        "expected NotFound for wrong-schema snapshot (schema filter hides it), got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_states_filters_by_account() {
    let harness: TypedResourceQueryServiceHarness = TypedResourceQueryServiceHarness::new();
    let account_handle_a = odf::AccountHandle::new_test("test-owner-a");
    let account_handle_b = odf::AccountHandle::new_test("test-owner-b");

    for name in ["res-a1", "res-a2", "res-a3"] {
        harness.apply_and_get_id(&account_handle_a, name).await;
    }

    for name in ["res-b1", "res-b2"] {
        harness.apply_and_get_id(&account_handle_b, name).await;
    }

    let states_a = harness
        .typed_query_svc()
        .list_states(
            account_handle_a.did,
            PaginationOpts::from_max_results(usize::MAX),
            ResolvedResourceLabelFilter::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(states_a.len(), 3, "expected 3 resources for account_a");

    let states_b = harness
        .typed_query_svc()
        .list_states(
            account_handle_b.did,
            PaginationOpts::from_max_results(usize::MAX),
            ResolvedResourceLabelFilter::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(states_b.len(), 2, "expected 2 resources for account_b");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ensure_id_matches_type_success() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let result = harness
        .typed_query_svc()
        .ensure_resource_id_matches_type(&id)
        .await;

    assert!(result.is_ok(), "expected Ok(()), got {result:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ensure_id_matches_type_mismatch() {
    let harness = TypedResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot_with_schema(id, &account_handle, &OTHER_SCHEMA, "res-a")
        .await;

    let result = harness
        .typed_query_svc()
        .ensure_resource_id_matches_type(&id)
        .await;

    assert!(
        matches!(result, Err(TypedResourceQueryError::TypeMismatch(_))),
        "expected TypeMismatch, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct TypedResourceQueryServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl TypedResourceQueryServiceHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        Self {
            base,
            catalog: b.build(),
        }
    }

    fn apply_svc(&self) -> Arc<dyn ApplyResourceUseCase<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    fn typed_query_svc(&self) -> Arc<dyn TypedResourceQueryService<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    async fn apply_and_get_id(&self, owner_handle: &odf::AccountHandle, name: &str) -> ResourceID {
        use kamu_resources::ApplyResourceApplicationDecision;

        let params = make_resource_params(owner_handle.clone(), name);
        let decision = self.apply_svc().apply(params).await.unwrap();

        match decision {
            ApplyResourceApplicationDecision::Applied(result) => result.id,
            ApplyResourceApplicationDecision::Rejected(rejection) => {
                panic!("apply rejected: {:?}", rejection.message)
            }
        }
    }

    async fn insert_snapshot_with_schema(
        &self,
        id: ResourceID,
        owner_handle: &odf::AccountHandle,
        schema: &TypeUri,
        name: &str,
    ) {
        let snapshot = ResourceSnapshot {
            id,
            schema: schema.clone(),
            headers: ResourceHeaders::simple(Utc::now(), id, owner_handle.clone(), name),
            spec: serde_json::json!({"value": name}),
            status: None,
            last_event_id: None,
        };

        self.resource_repo()
            .create_resource(&snapshot)
            .await
            .unwrap();
    }

    async fn allocate_id(&self) -> ResourceID {
        use kamu_resources::GenericResourceQueryService;
        self.catalog
            .get_one::<dyn GenericResourceQueryService>()
            .unwrap()
            .allocate_id()
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
