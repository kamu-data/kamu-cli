// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyResourceOutcome,
    Resource,
    ResourceHandle,
    ResourceID,
    ResourceSummaryView,
    TypeUri,
};
use kamu_resources_facade::BatchResourceResponse;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asserts the stable semantic fields of a `Resource` without comparing
/// volatile timestamps or exact UID values.
#[track_caller]
pub fn assert_resource_view_fields(
    view: &Resource,
    expected_schema: &TypeUri,
    expected_name: &str,
) {
    assert_eq!(view.schema, *expected_schema, "schema mismatch");
    assert_eq!(view.headers.name, expected_name, "name mismatch");
    assert!(
        view.headers.deleted_at.is_none(),
        "resource should not be deleted"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asserts the stable fields of a `ResourceHandle`.
#[track_caller]
pub fn assert_handle_fields(
    handle: &ResourceHandle,
    expected_schema: &TypeUri,
    expected_name: &str,
    expected_uid: &ResourceID,
) {
    assert_eq!(handle.r#type, *expected_schema, "handle schema mismatch");
    assert_eq!(handle.name, expected_name, "handle name mismatch");
    assert_eq!(handle.id, *expected_uid, "handle id mismatch");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asserts that an `ApplyManifestPlanningDecision` is `Planned` with the
/// expected `ApplyResourceOutcome`.
#[track_caller]
pub fn assert_planning_outcome(
    decision: &ApplyManifestPlanningDecision,
    expected_outcome: ApplyResourceOutcome,
) {
    let ApplyManifestPlanningDecision::Planned(plan) = decision else {
        panic!("expected Planned decision, got Rejected");
    };
    assert_eq!(plan.outcome, expected_outcome, "planning outcome mismatch");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asserts that an `ApplyManifestApplicationDecision` is `Applied` with the
/// expected `ApplyResourceOutcome` and returns the embedded `Resource`.
#[track_caller]
pub fn assert_applied_outcome(
    decision: &ApplyManifestApplicationDecision,
    expected_outcome: ApplyResourceOutcome,
) -> &Resource {
    let ApplyManifestApplicationDecision::Applied(result) = decision else {
        panic!("expected Applied decision, got Rejected");
    };
    assert_eq!(result.outcome, expected_outcome, "apply outcome mismatch");
    &result.resource
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Returns the single success item from a batch response, panicking if the
/// batch contains anything other than exactly one success and zero problems.
#[expect(dead_code)]
#[track_caller]
pub fn assert_single_batch_success<T: std::fmt::Debug, E: std::fmt::Debug>(
    response: BatchResourceResponse<T, E>,
) -> T {
    assert!(
        response.problems.is_empty(),
        "expected no batch problems, got: {:#?}",
        response.problems
    );
    assert_eq!(
        response.successes.len(),
        1,
        "expected exactly one success, got: {:#?}",
        response.successes
    );
    response.successes.into_iter().next().unwrap().item
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asserts that `request_index` values in successes and problems match
/// `expected_success_indexes` and `expected_problem_indexes` respectively,
/// independent of order within each list.
#[track_caller]
pub fn assert_batch_indexes<T, E>(
    response: &BatchResourceResponse<T, E>,
    expected_success_indexes: &[usize],
    expected_problem_indexes: &[usize],
) {
    let mut actual_success: Vec<usize> =
        response.successes.iter().map(|s| s.request_index).collect();
    let mut actual_problem: Vec<usize> =
        response.problems.iter().map(|p| p.request_index).collect();
    actual_success.sort_unstable();
    actual_problem.sort_unstable();

    let mut exp_success = expected_success_indexes.to_vec();
    let mut exp_problem = expected_problem_indexes.to_vec();
    exp_success.sort_unstable();
    exp_problem.sort_unstable();

    assert_eq!(actual_success, exp_success, "success indexes mismatch");
    assert_eq!(actual_problem, exp_problem, "problem indexes mismatch");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Normalizes a slice of `ResourceSummaryView` by sorting by `(schema, name)`.
pub fn normalize_summary_views(views: &mut [ResourceSummaryView]) {
    views.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
}

/// Normalizes a slice of `ResourceHandle` by sorting by `(schema, name)`.
pub fn normalize_handles(views: &mut [ResourceHandle]) {
    views.sort_by(|a, b| (&a.r#type, &a.name).cmp(&(&b.r#type, &b.name)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parses a JSON manifest string into a `serde_json::Value` for semantic
/// comparison.
#[expect(dead_code)]
pub fn parse_manifest_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).expect("manifest is not valid JSON")
}

/// Parses a YAML manifest string into a `serde_json::Value` via
/// YAML→JSON round-trip for semantic comparison.
#[expect(dead_code)]
pub fn parse_manifest_yaml(s: &str) -> serde_json::Value {
    let yaml_value: serde_yaml::Value =
        serde_yaml::from_str(s).expect("manifest is not valid YAML");
    serde_json::to_value(yaml_value).expect("YAML→JSON conversion failed")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
