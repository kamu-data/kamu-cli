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

use internal_error::ErrorIntoInternal;
use kamu_accounts::Principal;
use kamu_datasets::{
    ClassifyByAllowanceDatasetPermissionError,
    ClassifyByAllowanceResponse,
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionChecker,
    DatasetPermissionError,
    MockDatasetPermissionChecker,
};
use kamu_datasets_services::DatasetOperationGuardImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_handle_left_out_of_classification_is_internal_error() {
    let harness = DatasetOperationGuardHarness::leaving_handles_unclassified();

    assert_matches!(
        harness.check(DatasetOperation::Compaction).await,
        Err(DatasetPermissionError::Internal(e)) if format!("{e:?}").contains("was not classified")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_handle_classified_as_internal_is_internal_error() {
    let harness = DatasetOperationGuardHarness::classifying_as_internal();

    assert_matches!(
        harness.check(DatasetOperation::Compaction).await,
        Err(DatasetPermissionError::Internal(e)) if format!("{e:?}").contains("Classification failed")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetOperationGuardHarness {
    operation_guard: Arc<dyn DatasetOperationGuard>,
}

impl DatasetOperationGuardHarness {
    /// The checker answers without mentioning any of the handles it was given
    fn leaving_handles_unclassified() -> Self {
        Self::new(|_| ClassifyByAllowanceResponse {
            authorized_handles: Vec::new(),
            unauthorized_handles_with_errors: Vec::new(),
        })
    }

    fn classifying_as_internal() -> Self {
        Self::new(|handles| ClassifyByAllowanceResponse {
            authorized_handles: Vec::new(),
            unauthorized_handles_with_errors: handles
                .into_iter()
                .map(|handle| {
                    (
                        handle,
                        ClassifyByAllowanceDatasetPermissionError::Internal(
                            "Classification failed".int_err(),
                        ),
                    )
                })
                .collect(),
        })
    }

    fn new(
        classify: impl Fn(Vec<odf::DatasetHandle>) -> ClassifyByAllowanceResponse + Send + 'static,
    ) -> Self {
        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance()
            .returning(move |_, handles, _| Ok(classify(handles)));

        let catalog = dill::CatalogBuilder::new()
            .add_value(mock_dataset_permission_checker)
            .bind::<dyn DatasetPermissionChecker, MockDatasetPermissionChecker>()
            .add::<DatasetOperationGuardImpl>()
            .build();

        Self {
            operation_guard: catalog.get_one().unwrap(),
        }
    }

    async fn check(&self, operation: DatasetOperation) -> Result<(), DatasetPermissionError> {
        let target = odf::metadata::testing::handle(&"owner", &"foo", odf::DatasetKind::Root);

        self.operation_guard
            .check_operation(
                &Principal::new_test(),
                &DatasetOperationRequest::new(target, operation),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
