// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::{ClassifyDatasetRefsByAccessResponse, RebacDatasetRefUnresolvedError};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ExpectedResult<'a> {
    skipping: Option<&'a str>,
    not_skipping: Option<&'a str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case::all_empty(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![],
        forbidden: vec![],
        insufficient: vec![],
        allowed: vec![],
    },
    ExpectedResult { skipping: None, not_skipping: None }
)]
#[case::not_found_only(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![not_found_entry(1)],
        forbidden: vec![],
        insufficient: vec![],
        allowed: vec![],
    },
    ExpectedResult {
        skipping: None,
        not_skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: []
            - unresolved: [user/dataset-1-not-found]
        "}),
    }
)]
#[case::forbidden_only(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![],
        forbidden: vec![forbidden_entry(1)],
        insufficient: vec![],
        allowed: vec![],
    },
    ExpectedResult {
        skipping: None,
        not_skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: []
            - unresolved: [user/dataset-1-forbidden]
        "}),
    }
)]
#[case::both_missing(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![not_found_entry(1)],
        forbidden: vec![forbidden_entry(2)],
        insufficient: vec![],
        allowed: vec![],
    },
    ExpectedResult {
        skipping: None,
        not_skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: []
            - unresolved: [user/dataset-1-not-found,user/dataset-2-forbidden]
        "}),
    }
)]
#[case::insufficient_only(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![],
        forbidden: vec![],
        insufficient: vec![insufficient_entry(1)],
        allowed: vec![],
    },
    ExpectedResult {
        skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: [user/dataset-1-insufficient]
            - unresolved: []
        "}),
        not_skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: [user/dataset-1-insufficient]
            - unresolved: []
        "}),
    }
)]
#[case::allowed_only(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![],
        forbidden: vec![],
        insufficient: vec![],
        allowed: vec![allowed_entry(1)],
    },
    ExpectedResult { skipping: None, not_skipping: None }
)]
#[case::all_non_empty(
    ClassifyDatasetRefsByAccessResponse {
        not_found: vec![not_found_entry(1)],
        forbidden: vec![forbidden_entry(2)],
        insufficient: vec![insufficient_entry(3)],
        allowed: vec![allowed_entry(4)],
    },
    ExpectedResult {
        skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: [user/dataset-3-insufficient]
            - unresolved: []
        "}),
        not_skipping: Some(indoc::indoc! {"
            Dataset access error:
            - insufficient access level: [user/dataset-3-insufficient]
            - unresolved: [user/dataset-1-not-found,user/dataset-2-forbidden]
        "}),
    }
)]
fn test_try_get_user_error_report(
    #[case] response: ClassifyDatasetRefsByAccessResponse,
    #[case] expected: ExpectedResult<'_>,
) {
    assert_eq!(
        expected.not_skipping.map(str::to_string),
        response.try_get_user_error_report(false),
    );
    assert_eq!(
        expected.skipping.map(str::to_string),
        response.try_get_user_error_report(true),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset_ref(name: &str) -> odf::DatasetRef {
    dataset_handle(name).as_local_ref()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset_handle(name: &str) -> odf::DatasetHandle {
    odf::metadata::testing::handle(&"user", &name, odf::DatasetKind::Root)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn allowed_entry(index: usize) -> (odf::DatasetRef, odf::DatasetHandle) {
    let dataset_name = format!("dataset-{index}-allowed");

    (dataset_ref(&dataset_name), dataset_handle(&dataset_name))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn not_found_entry(index: usize) -> (odf::DatasetRef, odf::DatasetRefUnresolvedError) {
    let dataset_name = format!("dataset-{index}-not-found");
    let dataset_ref = dataset_ref(&dataset_name);

    let error = odf::DatasetRefUnresolvedError::NotFound(odf::DatasetNotFoundError {
        dataset_ref: dataset_ref.clone(),
    });

    (dataset_ref, error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn forbidden_entry(index: usize) -> (odf::DatasetRef, RebacDatasetRefUnresolvedError) {
    let dataset_name = format!("dataset-{index}-forbidden");
    let dataset_ref = dataset_ref(&dataset_name);

    let error = RebacDatasetRefUnresolvedError::Access(odf::AccessError::Unauthenticated(
        "No access".into(),
    ));

    (dataset_ref, error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn insufficient_entry(index: usize) -> (odf::DatasetRef, odf::DatasetHandle) {
    let dataset_name = format!("dataset-{index}-insufficient");

    (dataset_ref(&dataset_name), dataset_handle(&dataset_name))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
