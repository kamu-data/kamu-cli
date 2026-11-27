// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use kamu_resources::{
    ApplyManifestPlan,
    ApplyManifestResult,
    ApplyResourceOutcome,
    ResourceID,
    ResourceRef,
    ResourceSchemaProvider,
    ResourceVisibility,
};
use kamu_resources_facade::{
    ApplyManifestBatchRequest,
    ApplyManifestError,
    ApplyManifestRequest,
    ResourceFacade,
    ResourceHeadersValidationProblemCode,
    ResourceInvalidHeadersError,
    ResourceManifestFormat,
    SpecViewOpts,
};
use kamu_resources_services::testing::TestResource;
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, LocalFacadeHarnessOpts, TestAccount};
use crate::helpers::{VARIABLE_SET_SCHEMA_STR, assert_single_batch_success, create_variable_set};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-187
contract_test!(
    apply_without_name_creates_each_time,
    super::test_apply_without_name_creates_each_time,
    opts = super::LocalFacadeHarnessOpts::with_test_resources()
);

pub async fn test_apply_without_name_creates_each_time(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let mut results = vec![
        apply(facade.as_ref(), nameless_manifest(None, "same")).await,
        apply(facade.as_ref(), nameless_manifest(None, "same")).await,
    ];
    results.extend(
        apply_batch(
            facade.as_ref(),
            vec![
                nameless_manifest(None, "same"),
                nameless_manifest(None, "same"),
            ],
        )
        .await,
    );
    assert_eq!(results.len(), 4);

    for result in &results {
        assert_eq!(result.outcome, ApplyResourceOutcome::Created);
        let name = &result.resource.headers.name;
        assert!(name.as_str().starts_with("plain-"), "{name}");
        assert_eq!(
            result.documents().unwrap().after["headers"]["name"],
            name.as_str()
        );
        assert_eq!(
            &stored_name(facade.as_ref(), result.resource.headers.id).await,
            name.as_str()
        );
    }
    let ids: HashSet<_> = results.iter().map(|r| r.resource.headers.id).collect();
    assert_eq!(ids.len(), results.len());
    let names: HashSet<_> = results
        .iter()
        .map(|r| r.resource.headers.name.to_string())
        .collect();
    assert_eq!(names.len(), results.len());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-188
contract_test!(
    plan_without_name_leaves_it_pending,
    super::test_plan_without_name_leaves_it_pending,
    opts = super::LocalFacadeHarnessOpts::with_test_resources()
);

pub async fn test_plan_without_name_leaves_it_pending(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let pending = plan(facade.as_ref(), nameless_manifest(None, "value")).await;
    assert_eq!(pending.outcome, ApplyResourceOutcome::Created);
    assert!(pending.name_pending);
    assert_eq!(pending.resource.headers.name.as_str(), "plain-xxxxxx");
    let documents = pending.documents().unwrap();
    assert_eq!(documents.before, None);
    assert_eq!(documents.after["headers"].get("name"), None);

    let named = plan(facade.as_ref(), named_manifest("given-name", "value")).await;
    assert!(!named.name_pending);
    assert_eq!(
        named.documents().unwrap().after["headers"]["name"],
        "given-name"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-189
contract_test!(
    id_without_name_keeps_the_name,
    super::test_id_without_name_keeps_the_name,
    opts = super::LocalFacadeHarnessOpts::with_test_resources()
);

pub async fn test_id_without_name_keeps_the_name(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let created = apply(facade.as_ref(), named_manifest("given-name", "one")).await;
    let id = created.resource.headers.id;

    let planned = plan(facade.as_ref(), nameless_manifest(Some(id), "two")).await;
    assert_eq!(planned.outcome, ApplyResourceOutcome::Updated);
    assert!(!planned.name_pending);

    let updated = apply(facade.as_ref(), nameless_manifest(Some(id), "two")).await;
    assert_eq!(updated.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(updated.resource.headers.id, id);
    assert_eq!(updated.resource.headers.name.as_str(), "given-name");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-190
contract_test!(
    name_is_required_where_the_type_does_not_generate_it,
    super::test_name_is_required_where_the_type_does_not_generate_it
);

pub async fn test_name_is_required_where_the_type_does_not_generate_it(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    let id = create_variable_set(h, TestAccount::Alice, "existing").await;

    for headers in [
        serde_json::json!({}),
        serde_json::json!({ "name": null }),
        serde_json::json!({ "id": id.to_string() }),
    ] {
        let request = || {
            request(&serde_json::json!({
                "$schema": VARIABLE_SET_SCHEMA_STR,
                "headers": headers,
                "spec": { "variables": { "A": { "value": "1" } } },
            }))
        };

        assert_missing_name(facade.plan_apply_manifest(request()).await);
        assert_missing_name(facade.apply_manifest(request()).await);

        let mut batch = facade
            .apply_manifests(ApplyManifestBatchRequest {
                items: vec![request()],
            })
            .await
            .unwrap();
        assert_missing_name(batch.items.remove(0).outcome);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn request(manifest: &serde_json::Value) -> ApplyManifestRequest {
    ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: manifest.to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn nameless_manifest(id: Option<ResourceID>, value: &str) -> ApplyManifestRequest {
    request(&serde_json::json!({
        "$schema": TestResource::SCHEMA_STR,
        "headers": { "id": id.map(|id| id.to_string()) },
        "spec": { "value": value },
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn named_manifest(name: &str, value: &str) -> ApplyManifestRequest {
    request(&serde_json::json!({
        "$schema": TestResource::SCHEMA_STR,
        "headers": { "name": name },
        "spec": { "value": value },
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn plan(facade: &dyn ResourceFacade, request: ApplyManifestRequest) -> ApplyManifestPlan {
    facade
        .plan_apply_manifest(request)
        .await
        .unwrap()
        .expect_planned()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn apply(facade: &dyn ResourceFacade, request: ApplyManifestRequest) -> ApplyManifestResult {
    facade
        .apply_manifest(request)
        .await
        .unwrap()
        .expect_applied()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn apply_batch(
    facade: &dyn ResourceFacade,
    items: Vec<ApplyManifestRequest>,
) -> Vec<ApplyManifestResult> {
    let response = facade
        .apply_manifests(ApplyManifestBatchRequest { items })
        .await
        .unwrap();
    assert_eq!(response.rolled_back_successes, Vec::<usize>::new());
    response
        .items
        .into_iter()
        .map(|item| item.outcome.unwrap().expect_applied())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn stored_name(facade: &dyn ResourceFacade, id: ResourceID) -> String {
    let response = facade
        .get(
            vec![ResourceRef {
                account: None,
                r#type: Some(TestResource::type_uri().cheap_clone().into()),
                id: Some(id),
                did: None,
                name: None,
            }],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    assert_single_batch_success(response)
        .headers
        .name
        .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_missing_name(result: Result<impl std::fmt::Debug, ApplyManifestError>) {
    assert_matches!(
        result,
        Err(ApplyManifestError::InvalidHeaders(ResourceInvalidHeadersError {
            code: ResourceHeadersValidationProblemCode::MissingName,
            message,
        })) if message.contains(VARIABLE_SET_SCHEMA_STR)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
