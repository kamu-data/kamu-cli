// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
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

// TODO: Private Datasets: cover new paths

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_single_dataset_permissions() {
    use Caller::*;
    use DatasetPermission::*;
    use odf::DatasetVisibility::{Private, Public};

    const ALL: &[DatasetPermission] = &[Read, Write, Maintain, Own];

    let cases = [
        SingleDatasetCase::created(Owner, Private, ALL),
        SingleDatasetCase::created(Owner, Public, ALL),
        SingleDatasetCase::created(Anonymous, Public, &[Read]),
        SingleDatasetCase::created(Anonymous, Private, &[]),
        SingleDatasetCase::created(NotOwner, Public, &[Read]),
        SingleDatasetCase::created(NotOwner, Private, &[]),
        SingleDatasetCase::created(Admin, Public, ALL),
        SingleDatasetCase::created(Admin, Private, ALL),
        SingleDatasetCase::created(Node, Private, ALL),
        SingleDatasetCase::missing(Node),
        SingleDatasetCase::missing(Owner),
    ];

    for case in cases {
        case.assert_permissions().await;
    }
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
// Single-dataset cases
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Who asks for the permissions; the dataset belongs to `owner`
#[derive(Debug, Clone, Copy)]
enum Caller {
    Owner,
    NotOwner,
    Anonymous,
    Admin,
    Node,
}

/// One caller and one dataset, either created with a visibility or never
/// created. A created dataset grants the listed permissions, and the read and
/// write checks pass exactly when those include `Read` and `Write`
#[derive(Debug)]
struct SingleDatasetCase {
    caller: Caller,
    dataset: Option<(odf::DatasetVisibility, &'static [DatasetPermission])>,
}

impl SingleDatasetCase {
    fn created(
        caller: Caller,
        visibility: odf::DatasetVisibility,
        granted: &'static [DatasetPermission],
    ) -> Self {
        Self {
            caller,
            dataset: Some((visibility, granted)),
        }
    }

    fn missing(caller: Caller) -> Self {
        Self {
            caller,
            dataset: None,
        }
    }

    async fn assert_permissions(&self) {
        let harness = DatasetPermissionCheckerHarness::for_caller(self.caller).await;

        match self.dataset {
            Some((visibility, granted)) => {
                let dataset_handle =
                    odf::metadata::testing::handle(&"owner", &"dataset", odf::DatasetKind::Root);
                harness.create_dataset(&dataset_handle, visibility).await;
                let dataset_id = &dataset_handle.id;

                for permission in [DatasetPermission::Read, DatasetPermission::Write] {
                    let result = harness.check_permission(dataset_id, permission).await;
                    if granted.contains(&permission) {
                        assert_matches!(result, Ok(()), "{self:?}, {permission}");
                    } else {
                        assert_matches!(
                            result,
                            Err(DatasetPermissionError::Access(
                                odf::AccessError::Unauthorized(_)
                            )),
                            "{self:?}, {permission}"
                        );
                    }
                }
                assert_matches!(
                    harness.granted_permissions(dataset_id).await,
                    Ok(actual) if actual == granted.iter().copied().collect(),
                    "{self:?}"
                );
            }
            None => {
                let dataset_id = odf::DatasetID::new_seeded_ed25519(b"not_found_dataset_id");
                let dataset_ref = dataset_id.as_local_ref();

                for permission in [DatasetPermission::Read, DatasetPermission::Write] {
                    assert_matches!(
                        harness.check_permission(&dataset_id, permission).await,
                        Err(DatasetPermissionError::NotFound(e)) if e.dataset_ref == dataset_ref,
                        "{self:?}, {permission}"
                    );
                }
                assert_matches!(
                    harness.granted_permissions(&dataset_id).await,
                    Err(GetGrantedPermissionsError::NotFound(e)) if e.dataset_ref == dataset_ref,
                    "{self:?}"
                );
            }
        }
    }
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

    async fn for_caller(caller: Caller) -> Self {
        match caller {
            Caller::Owner => {
                Self::new(CurrentAccountSubjectTestHelper::logged("owner"), false).await
            }
            Caller::NotOwner => {
                Self::new(CurrentAccountSubjectTestHelper::logged("not-owner"), false).await
            }
            Caller::Anonymous => {
                Self::new(CurrentAccountSubjectTestHelper::anonymous(), false).await
            }
            Caller::Admin => {
                Self::new(CurrentAccountSubjectTestHelper::logged("admin"), true).await
            }
            Caller::Node => Self::new_node().await,
        }
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

    async fn check_permission(
        &self,
        dataset_id: &odf::DatasetID,
        permission: DatasetPermission,
    ) -> Result<(), DatasetPermissionError> {
        self.dataset_permission_checker
            .check_permission(&self.principal, dataset_id, permission)
            .await
    }

    async fn granted_permissions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, GetGrantedPermissionsError> {
        self.dataset_permission_checker
            .get_granted_permissions(&self.principal, dataset_id)
            .await
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
