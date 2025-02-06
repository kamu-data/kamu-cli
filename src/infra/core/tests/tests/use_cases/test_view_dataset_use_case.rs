// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::ViewDatasetUseCaseImpl;
use kamu_accounts::testing::CurrentAccountSubjectHelper;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::testing::{OwnerByAliasDatasetActionAuthorizer, ViewMultiResponseTestHelper};
use kamu_core::{TenancyConfig, ViewDatasetUseCase, ViewDatasetUseCaseError};

use crate::tests::use_cases::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_try_to_view_a_nonexistent_dataset() {
    let subjects = [
        CurrentAccountSubjectHelper::anonymous(),
        CurrentAccountSubjectHelper::logged("alice"),
        CurrentAccountSubjectHelper::logged("bob"),
    ];

    let nonexistent_dataset_alias = odf::metadata::testing::alias("alice", "foo");

    for subject in subjects {
        let harness = ViewDatasetUseCaseHarness::new(subject);

        assert_matches!(
            harness
                .use_case
                .execute(&nonexistent_dataset_alias.as_local_ref())
                .await,
            Err(ViewDatasetUseCaseError::NotFound(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_single_dataset() {
    enum ExpectedResult {
        Ok,
        AccessDenied,
    }

    let owner_subject = CurrentAccountSubjectHelper::logged("alice");
    let subjects_with_expected_results = [
        (
            CurrentAccountSubjectHelper::anonymous(),
            ExpectedResult::AccessDenied,
        ),
        (owner_subject, ExpectedResult::Ok),
        (
            CurrentAccountSubjectHelper::logged("bob"),
            ExpectedResult::AccessDenied,
        ),
    ];

    let dataset_alias = odf::metadata::testing::alias("alice", "foo");

    for (subject, expected_result) in subjects_with_expected_results {
        let harness = ViewDatasetUseCaseHarness::new(subject);

        harness
            .base_harness
            .create_root_dataset(&dataset_alias)
            .await;

        let res = harness
            .use_case
            .execute(&dataset_alias.as_local_ref())
            .await;

        match expected_result {
            ExpectedResult::Ok => {
                assert_matches!(res, Ok(_));
            }
            ExpectedResult::AccessDenied => {
                assert_matches!(res, Err(ViewDatasetUseCaseError::Access(_)));
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_multi_datasets() {
    let subjects_with_expected_results = [
        (
            CurrentAccountSubjectHelper::anonymous(),
            indoc::indoc!(
                r#"
                viewable_resolved_refs:

                inaccessible_refs:
                - alice/dataset-1: Forbidden
                - alice/dataset-2: Forbidden
                - bob/dataset-3: Forbidden
                "#
            ),
        ),
        (
            CurrentAccountSubjectHelper::logged("alice"),
            indoc::indoc!(
                r#"
                viewable_resolved_refs:
                - alice/dataset-1
                - alice/dataset-2

                inaccessible_refs:
                - bob/dataset-3: Forbidden
                "#
            ),
        ),
        (
            CurrentAccountSubjectHelper::logged("bob"),
            indoc::indoc!(
                r#"
                viewable_resolved_refs:
                - bob/dataset-3

                inaccessible_refs:
                - alice/dataset-1: Forbidden
                - alice/dataset-2: Forbidden
                "#
            ),
        ),
    ];

    let dataset_aliases = [
        odf::metadata::testing::alias("alice", "dataset-1"),
        odf::metadata::testing::alias("alice", "dataset-2"),
        odf::metadata::testing::alias("bob", "dataset-3"),
    ];

    for (subject, expected_result) in subjects_with_expected_results {
        let harness = ViewDatasetUseCaseHarness::new(subject);

        for dataset_alias in &dataset_aliases {
            harness
                .base_harness
                .create_root_dataset(dataset_alias)
                .await;
        }

        let res = harness
            .use_case
            .execute_multi(
                dataset_aliases
                    .iter()
                    .map(odf::DatasetAlias::as_local_ref)
                    .collect(),
            )
            .await;

        assert!(res.is_ok(), "{res:?}");

        pretty_assertions::assert_eq!(
            expected_result,
            ViewMultiResponseTestHelper::report(res.unwrap())
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct ViewDatasetUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    base_harness: BaseUseCaseHarness,
    pub use_case: Arc<dyn ViewDatasetUseCase>,
}

impl ViewDatasetUseCaseHarness {
    fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        {
            let datasets_dir = temp_dir.path().join("datasets");
            std::fs::create_dir(&datasets_dir).unwrap();
        }
        let owner_account_name = current_account_subject.maybe_account_name().cloned();

        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_tenancy_config(TenancyConfig::MultiTenant)
                .with_current_account_subject(current_account_subject),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<ViewDatasetUseCaseImpl>()
            .add::<OwnerByAliasDatasetActionAuthorizer>()
            .add_value(owner_account_name)
            .build();

        Self {
            _temp_dir: temp_dir,
            base_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
