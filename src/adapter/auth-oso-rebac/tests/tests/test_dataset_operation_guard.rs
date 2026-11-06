// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{CurrentAccountSubject, CurrentAccountSubjectCatalogExt as _, Principal};
use kamu_auth_rebac::{AccountToDatasetRelation, RebacService};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetPermissionMissingError,
};
use kamu_datasets_services::DatasetOperationGuardImpl;
use pretty_assertions::{assert_eq, assert_matches};

use crate::harness::BaseOsoRebacHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_target_permission_of_each_operation_by_role() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    harness
        .grant("reader", AccountToDatasetRelation::Reader, &target)
        .await;
    harness
        .grant("editor", AccountToDatasetRelation::Editor, &target)
        .await;
    harness
        .grant("maintainer", AccountToDatasetRelation::Maintainer, &target)
        .await;

    // Operation, then whether a Reader, an Editor and a Maintainer may run it
    let expectations = [
        (
            DatasetOperation::Transform { inputs: vec![] },
            [false, true, true],
        ),
        (DatasetOperation::PollingIngest, [false, true, true]),
        (DatasetOperation::PushIngest, [false, true, true]),
        (DatasetOperation::SyncFrom, [false, true, true]),
        (DatasetOperation::SetWatermark, [false, true, true]),
        (DatasetOperation::SyncTo, [true, true, true]),
        (
            DatasetOperation::Verify {
                replayed_inputs: vec![],
            },
            [true, true, true],
        ),
        (DatasetOperation::Compaction, [false, false, true]),
        (DatasetOperation::ResetToMetadata, [false, false, true]),
        (DatasetOperation::Reset, [false, false, true]),
    ];

    for (operation, allowed_by_role) in expectations {
        for (principal, expected_allowed) in ["reader", "editor", "maintainer"]
            .into_iter()
            .zip(allowed_by_role)
        {
            let allowed = harness
                .authorize(principal, &target, operation.clone())
                .await
                .is_ok();
            assert_eq!(
                expected_allowed, allowed,
                "{principal} running {operation:?}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unreadable_input_is_not_found_naming_the_input() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    let readable = harness.create_private_dataset("owner", "readable").await;
    let unreadable = harness
        .create_private_dataset("stranger", "unreadable")
        .await;

    for operation in [
        DatasetOperation::Transform {
            inputs: vec![readable.clone(), unreadable.clone()],
        },
        DatasetOperation::Verify {
            replayed_inputs: vec![readable.clone(), unreadable.clone()],
        },
    ] {
        assert_matches!(
            harness.authorize("owner", &target, operation).await,
            Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == unreadable.as_local_ref()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_denied_target_is_not_found_unless_readable() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let public = harness.create_public_dataset("stranger", "public").await;
    let private = harness.create_private_dataset("stranger", "private").await;

    for operation in [
        DatasetOperation::PollingIngest,
        DatasetOperation::Compaction,
    ] {
        assert_eq!(
            Some(public.as_local_ref()),
            OperationGuardHarness::denied_dataset(
                &harness.authorize("owner", &public, operation.clone()).await
            ),
            "{operation:?} of a public dataset"
        );
        assert_matches!(
            harness.authorize("owner", &private, operation.clone()).await,
            Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == private.as_local_ref(),
            "{operation:?} of a private dataset"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_target_error_takes_precedence_over_input_error() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    let input = harness.create_private_dataset("owner", "input").await;
    harness
        .grant("reader", AccountToDatasetRelation::Reader, &target)
        .await;

    let result = harness
        .authorize(
            "reader",
            &target,
            DatasetOperation::Transform {
                inputs: vec![input],
            },
        )
        .await;

    assert_eq!(
        Some(target.as_local_ref()),
        OperationGuardHarness::denied_dataset(&result)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_target_is_not_found() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let missing = odf::metadata::testing::handle(&"owner", &"missing", odf::DatasetKind::Root);

    assert_matches!(
        harness
            .authorize("owner", &missing, DatasetOperation::PollingIngest)
            .await,
        Err(DatasetPermissionError::NotFound(e))
            if e.dataset_ref == missing.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_principal_is_the_given_one_not_the_current_account() {
    let owner_harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let owned = owner_harness.create_private_dataset("owner", "owned").await;

    assert_matches!(
        owner_harness
            .authorize("stranger", &owned, DatasetOperation::Compaction)
            .await,
        Err(DatasetPermissionError::NotFound(_))
    );

    let anonymous_harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    let owned = anonymous_harness
        .create_private_dataset("owner", "owned")
        .await;

    assert_matches!(
        anonymous_harness
            .authorize("owner", &owned, DatasetOperation::Compaction)
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_node_may_run_every_operation() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    let input = harness.create_private_dataset("stranger", "input").await;

    let operations = [
        DatasetOperation::Transform {
            inputs: vec![input.clone()],
        },
        DatasetOperation::PollingIngest,
        DatasetOperation::PushIngest,
        DatasetOperation::SyncFrom,
        DatasetOperation::SetWatermark,
        DatasetOperation::SyncTo,
        DatasetOperation::Verify {
            replayed_inputs: vec![input.clone()],
        },
        DatasetOperation::Compaction,
        DatasetOperation::ResetToMetadata,
        DatasetOperation::Reset,
    ];

    for operation in operations {
        assert_matches!(
            harness
                .authorize_as(&Principal::Node, &target, operation.clone())
                .await,
            Ok(()),
            "node running {operation:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_target_is_not_found_for_node() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let missing = odf::metadata::testing::handle(&"owner", &"missing", odf::DatasetKind::Root);

    assert_matches!(
        harness
            .authorize_as(&Principal::Node, &missing, DatasetOperation::Compaction)
            .await,
        Err(DatasetPermissionError::NotFound(e))
            if e.dataset_ref == missing.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batch_keeps_request_order() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let owned = harness.create_private_dataset("owner", "owned").await;
    let foreign = harness.create_private_dataset("stranger", "foreign").await;
    let missing = odf::metadata::testing::handle(&"owner", &"missing", odf::DatasetKind::Root);

    let results = harness
        .authorize_all(
            "owner",
            vec![
                DatasetOperationRequest::new(foreign.clone(), DatasetOperation::SyncTo),
                DatasetOperationRequest::new(
                    owned.clone(),
                    DatasetOperation::Transform {
                        inputs: vec![owned.clone()],
                    },
                ),
                DatasetOperationRequest::new(missing.clone(), DatasetOperation::SyncFrom),
                DatasetOperationRequest::new(owned.clone(), DatasetOperation::Reset),
            ],
        )
        .await;

    assert_matches!(
        results.as_slice(),
        [
            Err(DatasetPermissionError::NotFound(_)),
            Ok(()),
            Err(DatasetPermissionError::NotFound(_)),
            Ok(()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNTS: [&str; 5] = ["owner", "reader", "editor", "maintainer", "stranger"];

#[oop::extend(BaseOsoRebacHarness, base)]
struct OperationGuardHarness {
    base: BaseOsoRebacHarness,
    operation_guard: Arc<dyn DatasetOperationGuard>,
    rebac_service: Arc<dyn RebacService>,
}

impl OperationGuardHarness {
    /// `current_account_subject` is the catalog's subject; each check names
    /// its principal on its own
    async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let base =
            BaseOsoRebacHarness::new(BaseOsoRebacHarness::predefined_accounts(&ACCOUNTS), |b| {
                b.add_current_account_subject(current_account_subject);
            })
            .await;

        let catalog = dill::CatalogBuilder::new_chained(base.catalog())
            .add::<DatasetOperationGuardImpl>()
            .build();

        Self {
            operation_guard: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
            base,
        }
    }

    async fn grant(
        &self,
        account_name: &str,
        relation: AccountToDatasetRelation,
        dataset: &odf::DatasetHandle,
    ) {
        self.rebac_service
            .set_account_dataset_relation(
                &odf::metadata::testing::account_id(&account_name),
                relation,
                &dataset.id,
            )
            .await
            .unwrap();
    }

    async fn authorize(
        &self,
        principal: &str,
        target: &odf::DatasetHandle,
        operation: DatasetOperation,
    ) -> Result<(), DatasetPermissionError> {
        self.authorize_as(&BaseOsoRebacHarness::user(principal), target, operation)
            .await
    }

    async fn authorize_as(
        &self,
        principal: &Principal,
        target: &odf::DatasetHandle,
        operation: DatasetOperation,
    ) -> Result<(), DatasetPermissionError> {
        self.operation_guard
            .check_operation(
                principal,
                &DatasetOperationRequest::new(target.clone(), operation),
            )
            .await
    }

    async fn authorize_all(
        &self,
        principal: &str,
        requests: Vec<DatasetOperationRequest>,
    ) -> Vec<Result<(), DatasetPermissionError>> {
        self.operation_guard
            .check_operations(&BaseOsoRebacHarness::user(principal), &requests)
            .await
            .unwrap()
    }

    /// The dataset a permission denial names
    fn denied_dataset(result: &Result<(), DatasetPermissionError>) -> Option<odf::DatasetRef> {
        let Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(e))) = result else {
            return None;
        };
        e.downcast_ref::<DatasetPermissionMissingError>()
            .map(|e| e.dataset_ref.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
