// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{
    AccountConfig,
    AccountPropertyName,
    CurrentAccountSubject,
    PredefinedAccountsConfig,
    Principal,
};
use kamu_datasets::{
    DatasetPermission,
    DatasetPermissionChecker,
    DatasetPermissionError,
    GetGrantedPermissionsError,
};
use kamu_datasets_services::testing::ClassifyByAllowanceIdsResponseTestHelper;
use pretty_assertions::{assert_eq, assert_matches};

use crate::harness::BaseOsoRebacHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Macro
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! assert_single_dataset {
    (
        setup:
            $harness: expr,
            dataset_id = $dataset_id: expr,
        expected:
            read_result = $( $expected_read_pattern:pat_param )|+ $( if $expected_read_guard: expr )?,
            write_result = $( $expected_write_pattern:pat_param )|+ $( if $expected_write_guard: expr )?,
            allowed_actions_result = $( $expected_allowed_actions_pattern:pat_param )|+ $( if $expected_allowed_actions_guard: expr )?
    ) => {
        assert_matches!(
            $harness
                .dataset_permission_checker
                .check_permission(&$harness.principal, &$dataset_id, DatasetPermission::Read)
                .await,
            $( $expected_read_pattern )|+ $( if $expected_read_guard )?
        );
        assert_matches!(
            $harness
                .dataset_permission_checker
                .check_permission(&$harness.principal, &$dataset_id, DatasetPermission::Write)
                .await,
            $( $expected_write_pattern )|+ $( if $expected_write_guard )?
        );
        assert_matches!(
            $harness
                .dataset_permission_checker
                .get_granted_permissions(&$harness.principal, &$dataset_id)
                .await,
            $( $expected_allowed_actions_pattern )|+ $( if $expected_allowed_actions_guard )?
        );
    };
}

// TODO: Private Datasets: cover new paths

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_private_dataset() {
    let owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("owner"),
        false,
    )
    .await;
    harness
        .create_private_datasets(&[&owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = owned_private_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [
                    DatasetPermission::Read,
                    DatasetPermission::Write,
                    DatasetPermission::Maintain,
                    DatasetPermission::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_public_dataset() {
    let owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("owner"),
        false,
    )
    .await;
    harness
        .create_public_datasets(&[&owned_public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = owned_public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [
                    DatasetPermission::Read,
                    DatasetPermission::Write,
                    DatasetPermission::Maintain,
                    DatasetPermission::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write_public_dataset() {
    let public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetPermissionCheckerHarness::new(CurrentAccountSubjectTestHelper::anonymous(), false)
            .await;
    harness
        .create_public_datasets(&[&public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetPermission::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_not_read_and_write_private_dataset() {
    let private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetPermissionCheckerHarness::new(CurrentAccountSubjectTestHelper::anonymous(), false)
            .await;
    harness
        .create_private_datasets(&[&private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = private_dataset_handle.id,
        expected:
            read_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            write_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_read_but_not_write_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("not-owner"),
        false,
    )
    .await;

    harness
        .create_public_datasets(&[&not_owned_public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetPermission::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_not_read_and_write_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("not-owner"),
        false,
    )
    .await;

    harness
        .create_private_datasets(&[&not_owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_handle.id,
        expected:
            read_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            write_result = Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("admin"),
        true,
    )
    .await;

    harness
        .create_public_datasets(&[&not_owned_public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [
                    DatasetPermission::Read,
                    DatasetPermission::Write,
                    DatasetPermission::Maintain,
                    DatasetPermission::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("admin"),
        true,
    )
    .await;

    harness
        .create_private_datasets(&[&not_owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [
                    DatasetPermission::Read,
                    DatasetPermission::Write,
                    DatasetPermission::Maintain,
                    DatasetPermission::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_datasets_matrix() {
    struct ExpectedResults<'a> {
        read_filter_datasets_allowing_result: Vec<odf::DatasetHandle>,
        write_filter_datasets_allowing_result: Vec<odf::DatasetHandle>,
        read_classify_dataset_handles_by_allowance_result: &'a str,
        write_classify_dataset_handles_by_allowance_result: &'a str,
        read_classify_dataset_ids_by_allowance_result: &'a str,
        write_classify_dataset_ids_by_allowance_result: &'a str,
    }
    use odf::metadata::testing::handle;

    let alice_private_dataset_1_handle =
        handle(&"alice", &"private-dataset-1", odf::DatasetKind::Root);
    let alice_public_dataset_2_handle =
        handle(&"alice", &"public-dataset-2", odf::DatasetKind::Root);
    let bob_private_dataset_3_handle = handle(&"bob", &"private-dataset-3", odf::DatasetKind::Root);
    let bob_public_dataset_4_handle = handle(&"bob", &"public-dataset-4", odf::DatasetKind::Root);

    let all_dataset_handles = vec![
        alice_private_dataset_1_handle.clone(),
        alice_public_dataset_2_handle.clone(),
        bob_private_dataset_3_handle.clone(),
        bob_public_dataset_4_handle.clone(),
    ];
    let dataset_handle_map =
        all_dataset_handles
            .clone()
            .into_iter()
            .fold(HashMap::new(), |mut acc, h| {
                acc.insert(h.id, h.alias);
                acc
            });

    let all_dataset_ids = all_dataset_handles
        .iter()
        .map(|h| Cow::Borrowed(&h.id))
        .collect::<Vec<_>>();

    let subjects_with_expected_results = [
        (
            CurrentAccountSubjectTestHelper::anonymous(),
            false,
            ExpectedResults {
                read_filter_datasets_allowing_result: vec![
                    // alice_private_dataset_1_handle.clone(),
                    alice_public_dataset_2_handle.clone(),
                    // bob_private_dataset_3_handle.clone(),
                    bob_public_dataset_4_handle.clone(),
                ],
                write_filter_datasets_allowing_result: vec![],
                read_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Unauthorized
                    - alice/private-dataset-1: Unauthorized
                    "#
                ),
                write_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:

                    unauthorized_with_errors:
                    - alice/public-dataset-2: Unauthorized
                    - bob/public-dataset-4: Unauthorized
                    - bob/private-dataset-3: Unauthorized
                    - alice/private-dataset-1: Unauthorized
                    "#
                ),
                read_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Unauthorized
                    - alice/private-dataset-1: Unauthorized
                    "#
                ),
                write_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:

                    unauthorized_with_errors:
                    - alice/public-dataset-2: Unauthorized
                    - bob/public-dataset-4: Unauthorized
                    - bob/private-dataset-3: Unauthorized
                    - alice/private-dataset-1: Unauthorized
                    "#
                ),
            },
        ),
        (
            CurrentAccountSubjectTestHelper::logged("alice"),
            false,
            ExpectedResults {
                read_filter_datasets_allowing_result: vec![
                    alice_private_dataset_1_handle.clone(),
                    alice_public_dataset_2_handle.clone(),
                    // bob_private_dataset_3_handle.clone(),
                    bob_public_dataset_4_handle.clone(),
                ],
                write_filter_datasets_allowing_result: vec![
                    alice_private_dataset_1_handle.clone(),
                    alice_public_dataset_2_handle.clone(),
                    // bob_private_dataset_3_handle.clone(),
                    // bob_public_dataset_4_handle.clone(),
                ],
                read_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Unauthorized
                    "#
                ),
                write_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/public-dataset-4: Unauthorized
                    - bob/private-dataset-3: Unauthorized
                    "#
                ),
                read_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Unauthorized
                    "#
                ),
                write_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/public-dataset-4: Unauthorized
                    - bob/private-dataset-3: Unauthorized
                    "#
                ),
            },
        ),
        (
            CurrentAccountSubjectTestHelper::logged("admin"),
            true,
            ExpectedResults {
                read_filter_datasets_allowing_result: vec![
                    alice_private_dataset_1_handle.clone(),
                    alice_public_dataset_2_handle.clone(),
                    bob_private_dataset_3_handle.clone(),
                    bob_public_dataset_4_handle.clone(),
                ],
                write_filter_datasets_allowing_result: vec![
                    alice_private_dataset_1_handle.clone(),
                    alice_public_dataset_2_handle.clone(),
                    bob_private_dataset_3_handle.clone(),
                    bob_public_dataset_4_handle.clone(),
                ],
                read_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - bob/private-dataset-3
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    "#
                ),
                write_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - bob/private-dataset-3
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    "#
                ),
                read_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - bob/private-dataset-3
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    "#
                ),
                write_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - bob/private-dataset-3
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    "#
                ),
            },
        ),
    ];

    for (subject, is_admin, expected_results) in subjects_with_expected_results {
        let harness = DatasetPermissionCheckerHarness::new(subject, is_admin).await;

        harness
            .create_private_datasets(&[
                &alice_private_dataset_1_handle,
                &bob_private_dataset_3_handle,
            ])
            .await;
        harness
            .create_public_datasets(&[&alice_public_dataset_2_handle, &bob_public_dataset_4_handle])
            .await;

        assert_eq!(
            {
                let mut expected = expected_results.read_filter_datasets_allowing_result;
                expected.sort_by(|left, right| left.id.cmp(&right.id));
                expected
            },
            {
                let mut actual = harness
                    .dataset_permission_checker
                    .filter_datasets_allowing(
                        &harness.principal,
                        all_dataset_handles.clone(),
                        DatasetPermission::Read,
                    )
                    .await
                    .unwrap();
                actual.sort_by(|left, right| left.id.cmp(&right.id));
                actual
            }
        );
        assert_eq!(
            {
                let mut expected = expected_results.write_filter_datasets_allowing_result;
                expected.sort_by(|left, right| left.id.cmp(&right.id));
                expected
            },
            {
                let mut res = harness
                    .dataset_permission_checker
                    .filter_datasets_allowing(
                        &harness.principal,
                        all_dataset_handles.clone(),
                        DatasetPermission::Write,
                    )
                    .await
                    .unwrap();
                res.sort_by(|left, right| left.id.cmp(&right.id));
                res
            }
        );

        assert_eq!(
            expected_results.read_classify_dataset_handles_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_permission_checker
                    .classify_dataset_handles_by_allowance(
                        &harness.principal,
                        all_dataset_handles.clone(),
                        DatasetPermission::Read
                    )
                    .await
                    .unwrap()
                    .into(),
                &dataset_handle_map
            ),
        );
        assert_eq!(
            expected_results.write_classify_dataset_handles_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_permission_checker
                    .classify_dataset_handles_by_allowance(
                        &harness.principal,
                        all_dataset_handles.clone(),
                        DatasetPermission::Write
                    )
                    .await
                    .unwrap()
                    .into(),
                &dataset_handle_map
            ),
        );

        assert_eq!(
            expected_results.read_classify_dataset_ids_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_permission_checker
                    .classify_dataset_ids_by_allowance(
                        &harness.principal,
                        &all_dataset_ids,
                        DatasetPermission::Read
                    )
                    .await
                    .unwrap(),
                &dataset_handle_map
            ),
        );
        assert_eq!(
            expected_results.write_classify_dataset_ids_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_permission_checker
                    .classify_dataset_ids_by_allowance(
                        &harness.principal,
                        &all_dataset_ids,
                        DatasetPermission::Write
                    )
                    .await
                    .unwrap(),
                &dataset_handle_map
            ),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_node_can_do_everything_with_not_owned_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness = DatasetPermissionCheckerHarness::new_node().await;

    harness
        .create_private_datasets(&[&not_owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [
                    DatasetPermission::Read,
                    DatasetPermission::Write,
                    DatasetPermission::Maintain,
                    DatasetPermission::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_node_cannot_read_not_found_dataset() {
    let not_found_dataset_id = odf::DatasetID::new_seeded_ed25519(b"not_found_dataset_id");
    let not_found_dataset_ref = not_found_dataset_id.as_local_ref();

    let harness = DatasetPermissionCheckerHarness::new_node().await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_found_dataset_id,
        expected:
            read_result = Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref,
            write_result = Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref,
            allowed_actions_result = Err(GetGrantedPermissionsError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_nobody_cannot_read_not_found_dataset() {
    let not_found_dataset_id = odf::DatasetID::new_seeded_ed25519(b"not_found_dataset_id");
    let not_found_dataset_ref = not_found_dataset_id.as_local_ref();

    let harness = DatasetPermissionCheckerHarness::new(
        CurrentAccountSubjectTestHelper::logged("owner"),
        false,
    )
    .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_found_dataset_id,
        expected:
            read_result = Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref,
            write_result = Err(DatasetPermissionError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref,
            allowed_actions_result = Err(GetGrantedPermissionsError::NotFound(e))
                if e.dataset_ref == not_found_dataset_ref
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseOsoRebacHarness, base)]
struct DatasetPermissionCheckerHarness {
    base: BaseOsoRebacHarness,
    principal: Principal,
    dataset_permission_checker: Arc<dyn DatasetPermissionChecker>,
}

impl DatasetPermissionCheckerHarness {
    pub async fn new(current_account_subject: CurrentAccountSubject, is_admin: bool) -> Self {
        let principal = Principal::from(&current_account_subject);
        Self::build(current_account_subject, principal, is_admin).await
    }

    /// The node as the principal, while the catalog holds an anonymous caller
    pub async fn new_node() -> Self {
        Self::build(
            CurrentAccountSubjectTestHelper::anonymous(),
            Principal::Node,
            false,
        )
        .await
    }

    async fn build(
        current_account_subject: CurrentAccountSubject,
        principal: Principal,
        is_admin: bool,
    ) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();

        if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
            let mut account_properties = Vec::new();
            if is_admin {
                account_properties.push(AccountPropertyName::IsAdmin);
            }
            let account_config =
                AccountConfig::test_config_from_name_with_id(logged_account.name.clone())
                    .set_properties(account_properties);

            predefined_accounts_config.predefined.push(account_config);
        }

        let base = BaseOsoRebacHarness::new(predefined_accounts_config, |b| {
            current_account_subject.register(b);
        })
        .await;

        Self {
            principal,
            dataset_permission_checker: base.catalog().get_one().unwrap(),
            base,
        }
    }

    async fn create_public_datasets(&self, datasets_handles: &[&odf::DatasetHandle]) {
        for dataset_handle in datasets_handles {
            self.create_dataset(dataset_handle, odf::DatasetVisibility::Public)
                .await;
        }
    }

    async fn create_private_datasets(&self, datasets_handles: &[&odf::DatasetHandle]) {
        for dataset_handle in datasets_handles {
            self.create_dataset(dataset_handle, odf::DatasetVisibility::Private)
                .await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
