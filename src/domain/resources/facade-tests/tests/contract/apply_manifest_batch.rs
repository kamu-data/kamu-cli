// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::VariableSetResource;
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyManifestRejection,
    ApplyResourceOutcome,
    ApplyResourceRejectionCategory,
    ResourceRef,
    ResourceSchemaProvider,
    TypeName,
};
use kamu_resources_facade::{
    ApplyManifestBatchRequest,
    ApplyManifestBatchResponse,
    ApplyManifestError,
    ApplyManifestRequest,
    ResourceAccountResolutionProblemCode,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, RemoteGraphqlFacadeHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_SCHEMA_STR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_resource_view_fields,
    assert_single_batch_problem,
    assert_single_batch_success,
    secret_set_manifest_json,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn batch_request(manifests: Vec<String>) -> ApplyManifestBatchRequest {
    ApplyManifestBatchRequest {
        items: manifests
            .into_iter()
            .map(|manifest| ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest,
            })
            .collect(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn business_invalid_manifest(name: &str) -> String {
    serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {"name": name},
        "spec": {"variables": {}}
    })
    .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn malformed_manifest() -> String {
    "not valid json {{{".to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn variable_set_manifest_json_with_unknown_account(name: &str) -> String {
    serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": name,
            "account": {"name": "unknown-resource-contract-account"}
        },
        "spec": {
            "variables": {
                "K": {"value": "v"}
            }
        }
    })
    .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn variable_set_manifest_json_with_id(id: kamu_resources::ResourceID, name: &str) -> String {
    serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "id": id,
            "name": name
        },
        "spec": {
            "variables": {
                "K": {"value": "v"}
            }
        }
    })
    .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_absent(h: &impl FacadeContractHarness, name: &str) {
    let result = h
        .facade_for(TestAccount::Alice)
        .get(vec![by_name(name)], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();

    assert_matches!(
        assert_single_batch_problem(result),
        ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::AnyTypeNameNotFound(_),
        "resource '{name}' must not exist"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_batch_indexes<D>(
    response: &ApplyManifestBatchResponse<D>,
    expected_item_indexes: &[usize],
    expected_rolled_back_successes: &[usize],
) {
    let mut item_indexes = response
        .items
        .iter()
        .map(|item| item.request_index)
        .collect::<Vec<_>>();
    item_indexes.sort_unstable();

    let mut rolled_back_successes = response.rolled_back_successes.clone();
    rolled_back_successes.sort_unstable();

    assert_eq!(item_indexes, expected_item_indexes, "item indexes mismatch");
    assert_eq!(
        rolled_back_successes, expected_rolled_back_successes,
        "rolled-back success indexes mismatch"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_created_apply_item(decision: &ApplyManifestApplicationDecision, expected_name: &str) {
    let ApplyManifestApplicationDecision::Applied(result) = decision else {
        panic!("expected Applied decision, got: {decision:?}");
    };
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);
    assert_resource_view_fields(
        &result.resource,
        VariableSetResource::schema(),
        expected_name,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_rejected_apply_item(decision: &ApplyManifestApplicationDecision) {
    assert_matches!(
        decision,
        ApplyManifestApplicationDecision::Rejected(ApplyManifestRejection {
            category: ApplyResourceRejectionCategory::BusinessValidationFailed,
            ..
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_created_plan_item(decision: &ApplyManifestPlanningDecision) {
    assert_matches!(
        decision,
        ApplyManifestPlanningDecision::Planned(plan)
            if plan.outcome == ApplyResourceOutcome::Created
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_updated_apply_item(decision: &ApplyManifestApplicationDecision) {
    assert_matches!(
        decision,
        ApplyManifestApplicationDecision::Applied(result)
            if result.outcome == ApplyResourceOutcome::Updated
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_rejected_or_rolled_back_success<D, F>(
    response: &ApplyManifestBatchResponse<D>,
    rejected_index: usize,
    assert_rejection: F,
) where
    D: std::fmt::Debug,
    F: FnOnce(&D),
{
    assert_eq!(
        response.items.len(),
        1,
        "rollback response must expose only the rejected/failed item via items: {response:?}"
    );
    assert_eq!(response.items[0].request_index, rejected_index);
    assert_rejection(response.items[0].outcome.as_ref().unwrap());
    assert_batch_indexes(response, &[rejected_index], &[0]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_local_stop_response<D, F>(
    response: &ApplyManifestBatchResponse<D>,
    rejected_index: usize,
    assert_rejection: F,
) where
    D: std::fmt::Debug,
    F: FnOnce(&D),
{
    assert_batch_indexes(response, &[0, rejected_index], &[]);
    assert!(response.items[0].outcome.is_ok());
    assert_rejection(response.items[1].outcome.as_ref().unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_stop_on_rejection_shape<D, F>(
    response: &ApplyManifestBatchResponse<D>,
    rejected_index: usize,
    assert_rejection: F,
) where
    D: std::fmt::Debug,
    F: FnOnce(&D) + Copy,
{
    if response.rolled_back_successes.is_empty() {
        assert_local_stop_response(response, rejected_index, assert_rejection);
    } else {
        assert_rejected_or_rolled_back_success(response, rejected_index, assert_rejection);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-160
contract_test!(
    batch_apply_all_successes_preserves_order,
    super::test_batch_apply_all_successes_preserves_order
);

pub async fn test_batch_apply_all_successes_preserves_order(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-contract-a", None, &[("K", "1")]),
            variable_set_manifest_json("batch-contract-b", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_created_apply_item(
        response.items[0].outcome.as_ref().unwrap(),
        "batch-contract-a",
    );
    assert_created_apply_item(
        response.items[1].outcome.as_ref().unwrap(),
        "batch-contract-b",
    );

    let fetched = assert_single_batch_success(
        facade
            .get(vec![by_name("batch-contract-b")], SpecViewOpts::ENCRYPTED)
            .await
            .unwrap(),
    );
    assert_resource_view_fields(&fetched, VariableSetResource::schema(), "batch-contract-b");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-161
contract_test!(
    batch_apply_stops_on_business_rejection,
    super::test_batch_apply_stops_on_business_rejection
);

pub async fn test_batch_apply_stops_on_business_rejection(h: &impl FacadeContractHarness) {
    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-reject-before", None, &[("K", "1")]),
            business_invalid_manifest("batch-reject-invalid"),
            variable_set_manifest_json("batch-reject-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    assert_stop_on_rejection_shape(&response, 1, assert_rejected_apply_item);

    assert_absent(h, "batch-reject-invalid").await;
    assert_absent(h, "batch-reject-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-162
contract_test!(
    batch_apply_stops_on_hard_failure,
    super::test_batch_apply_stops_on_hard_failure
);

pub async fn test_batch_apply_stops_on_hard_failure(h: &impl FacadeContractHarness) {
    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-fail-before", None, &[("K", "1")]),
            malformed_manifest(),
            variable_set_manifest_json("batch-fail-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    if response.rolled_back_successes.is_empty() {
        assert_batch_indexes(&response, &[0, 1], &[]);
        assert!(response.items[0].outcome.is_ok());
        assert_matches!(
            response.items[1].outcome,
            Err(ApplyManifestError::ParseManifest(_))
        );
    } else {
        assert_batch_indexes(&response, &[1], &[0]);
        assert_matches!(
            response.items[0].outcome,
            Err(ApplyManifestError::ParseManifest(_))
        );
    }
    assert_absent(h, "batch-fail-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-163
contract_test!(
    batch_apply_rollback_reconstructs_id_not_found,
    super::test_batch_apply_rollback_reconstructs_id_not_found
);

pub async fn test_batch_apply_rollback_reconstructs_id_not_found(h: &impl FacadeContractHarness) {
    let missing_id = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-id-not-found-before", None, &[("K", "1")]),
            variable_set_manifest_json_with_id(missing_id, "batch-id-not-found-missing"),
            variable_set_manifest_json("batch-id-not-found-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    if response.rolled_back_successes.is_empty() {
        assert_batch_indexes(&response, &[0, 1], &[]);
        assert!(response.items[0].outcome.is_ok());
        assert_matches!(
            response.items[1].outcome,
            Err(ApplyManifestError::IDNotFound(err)) if err.0 == missing_id
        );
    } else {
        assert_batch_indexes(&response, &[1], &[0]);
        assert_matches!(
            response.items[0].outcome,
            Err(ApplyManifestError::IDNotFound(err)) if err.0 == missing_id
        );
    }
    assert_absent(h, "batch-id-not-found-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-178
// The rollback summary is a hand-maintained serde contract carried through
// GraphQL error extensions, with no compile-time link between the server that
// serializes it and the remote facade that decodes it — and a decode failure
// degrades silently to an opaque transport error. This pins the
// account-resolution arm of that contract so a drift between the two sides
// fails here rather than going unnoticed.
contract_test!(
    batch_apply_rollback_reconstructs_account_resolution,
    super::test_batch_apply_rollback_reconstructs_account_resolution
);

pub async fn test_batch_apply_rollback_reconstructs_account_resolution(
    h: &impl FacadeContractHarness,
) {
    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-acct-resolution-before", None, &[("K", "1")]),
            variable_set_manifest_json_with_unknown_account("batch-acct-resolution-unknown"),
            variable_set_manifest_json("batch-acct-resolution-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    let failed_outcome = if response.rolled_back_successes.is_empty() {
        assert_batch_indexes(&response, &[0, 1], &[]);
        assert!(response.items[0].outcome.is_ok());
        &response.items[1].outcome
    } else {
        assert_batch_indexes(&response, &[1], &[0]);
        &response.items[0].outcome
    };

    assert_matches!(
        failed_outcome,
        Err(ApplyManifestError::AccountResolution(err))
            if err.code == ResourceAccountResolutionProblemCode::AccountNotFoundByName,
        "unknown account in a batch must survive the rollback round-trip as a typed \
         AccountResolution error, got: {failed_outcome:?}"
    );
    assert_absent(h, "batch-acct-resolution-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-164
contract_test!(
    batch_apply_rollback_reconstructs_type_mismatch,
    super::test_batch_apply_rollback_reconstructs_type_mismatch
);

pub async fn test_batch_apply_rollback_reconstructs_type_mismatch(h: &impl FacadeContractHarness) {
    let secret_id = h
        .facade_for(TestAccount::Alice)
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: secret_set_manifest_json("batch-type-mismatch-secret", None, &[("K", "v")]),
        })
        .await
        .unwrap()
        .expect_applied()
        .resource
        .headers
        .id;

    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-type-mismatch-before", None, &[("K", "1")]),
            variable_set_manifest_json_with_id(secret_id, "batch-type-mismatch-variable"),
            variable_set_manifest_json("batch-type-mismatch-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    if response.rolled_back_successes.is_empty() {
        assert_batch_indexes(&response, &[0, 1], &[]);
        assert!(response.items[0].outcome.is_ok());
        assert_matches!(
            &response.items[1].outcome,
            Err(ApplyManifestError::TypeMismatch(err))
                if err.id == secret_id
                    && err.expected_schema.as_str() == VARIABLE_SET_SCHEMA_STR
                    && err.actual_schema.as_str() == SECRET_SET_SCHEMA_STR
        );
    } else {
        assert_batch_indexes(&response, &[1], &[0]);
        assert_matches!(
            &response.items[0].outcome,
            Err(ApplyManifestError::TypeMismatch(err))
                if err.id == secret_id
                    && err.expected_schema.as_str() == VARIABLE_SET_SCHEMA_STR
                    && err.actual_schema.as_str() == SECRET_SET_SCHEMA_STR
        );
    }
    assert_absent(h, "batch-type-mismatch-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-165
contract_test!(
    batch_plan_stops_on_business_rejection_without_persisting,
    super::test_batch_plan_stops_on_business_rejection
);

pub async fn test_batch_plan_stops_on_business_rejection(h: &impl FacadeContractHarness) {
    let response = h
        .facade_for(TestAccount::Alice)
        .plan_apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-plan-before", None, &[("K", "1")]),
            business_invalid_manifest("batch-plan-invalid"),
            variable_set_manifest_json("batch-plan-after", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    assert_stop_on_rejection_shape(&response, 1, |decision| {
        assert_matches!(
            decision,
            ApplyManifestPlanningDecision::Rejected(ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                ..
            })
        );
    });

    assert_absent(h, "batch-plan-before").await;
    assert_absent(h, "batch-plan-invalid").await;
    assert_absent(h, "batch-plan-after").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-166
contract_test!(
    batch_plan_create_then_update_same_name_plans_both_as_create,
    super::test_batch_plan_create_then_update_same_name_plans_both_as_create
);

pub async fn test_batch_plan_create_then_update_same_name_plans_both_as_create(
    h: &impl FacadeContractHarness,
) {
    let response = h
        .facade_for(TestAccount::Alice)
        .plan_apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-plan-same-name", None, &[("K", "1")]),
            variable_set_manifest_json("batch-plan-same-name", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_created_plan_item(response.items[0].outcome.as_ref().unwrap());
    assert_created_plan_item(response.items[1].outcome.as_ref().unwrap());
    assert_absent(h, "batch-plan-same-name").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-167
contract_test!(
    batch_apply_create_then_update_same_name_reads_own_writes,
    super::test_batch_apply_create_then_update_same_name_reads_own_writes
);

pub async fn test_batch_apply_create_then_update_same_name_reads_own_writes(
    h: &impl FacadeContractHarness,
) {
    let response = h
        .facade_for(TestAccount::Alice)
        .apply_manifests(batch_request(vec![
            variable_set_manifest_json("batch-apply-same-name", None, &[("K", "1")]),
            variable_set_manifest_json("batch-apply-same-name", None, &[("K", "2")]),
        ]))
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_created_apply_item(
        response.items[0].outcome.as_ref().unwrap(),
        "batch-apply-same-name",
    );
    assert_updated_apply_item(response.items[1].outcome.as_ref().unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-168
#[test_log::test(tokio::test)]
async fn raw_graphql_batch_rejection_returns_rollback_extensions() {
    let h = RemoteGraphqlFacadeHarness::new().await;
    let response = h
        .execute_raw_graphql(
            TestAccount::Alice,
            async_graphql::Request::new(indoc::indoc!(
                r#"
                mutation ApplyManifests($manifests: [ApplyManifestInput!]!) {
                  resources {
                    applyManifests(manifests: $manifests, dryRun: false) {
                      items {
                        requestIndex
                        outcome {
                          __typename
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(serde_json::json!({
                "manifests": [
                    {
                        "format": "JSON",
                        "manifest": variable_set_manifest_json(
                            "raw-gql-rollback-before",
                            None,
                            &[("K", "1")],
                        ),
                    },
                    {
                        "format": "JSON",
                        "manifest": business_invalid_manifest("raw-gql-rollback-invalid"),
                    },
                    {
                        "format": "JSON",
                        "manifest": variable_set_manifest_json(
                            "raw-gql-rollback-after",
                            None,
                            &[("K", "2")],
                        ),
                    },
                ]
            }))),
        )
        .await;

    let response_json = serde_json::to_value(&response).unwrap();
    assert!(
        !response.errors.is_empty(),
        "rollback path must be represented as a top-level GraphQL error: {response_json:#}"
    );
    assert_eq!(
        response_json["errors"][0]["extensions"]["batch"]["rolled_back_successes"],
        serde_json::json!([0])
    );
    assert_eq!(
        response_json["errors"][0]["extensions"]["batch"]["items"][0]["request_index"],
        serde_json::json!(1)
    );
    assert_eq!(
        response_json["errors"][0]["extensions"]["batch"]["items"][0]["outcome"]["kind"],
        serde_json::json!("Rejected")
    );
    assert_eq!(
        response_json["errors"][0]["extensions"]["batch"]["items"][0]["outcome"]["category"],
        serde_json::json!("BusinessValidationFailed")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
