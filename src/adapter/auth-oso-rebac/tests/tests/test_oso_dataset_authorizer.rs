// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;

use dill::Component;
use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{AccountConfig, CurrentAccountSubject, PredefinedAccountsConfig};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::testing::ClassifyByAllowanceIdsResponseTestHelper;
use kamu_core::TenancyConfig;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_services::{DatasetEntryServiceImpl, DatasetEntryWriter};
use messaging_outbox::{
    register_message_dispatcher,
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
};
use time_source::SystemTimeSourceDefault;

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
                .dataset_authorizer
                .check_action_allowed(&$dataset_id, DatasetAction::Read)
                .await,
            $( $expected_read_pattern )|+ $( if $expected_read_guard )?
        );
        assert_matches!(
            $harness
                .dataset_authorizer
                .check_action_allowed(&$dataset_id, DatasetAction::Write)
                .await,
            $( $expected_write_pattern )|+ $( if $expected_write_guard )?
        );
        assert_matches!(
            $harness
                .dataset_authorizer
                .get_allowed_actions(&$dataset_id)
                .await,
            $( $expected_allowed_actions_pattern )|+ $( if $expected_allowed_actions_guard )?
        );
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_private_dataset() {
    let owned_private_dataset_handle = odf::metadata::testing::handle(&"owner", &"private-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
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
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_public_dataset() {
    let owned_public_dataset_handle = odf::metadata::testing::handle(&"owner", &"public-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
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
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write_public_dataset() {
    let public_dataset_handle = odf::metadata::testing::handle(&"owner", &"public-dataset");

    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    harness
        .create_public_datasets(&[&public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_not_read_and_write_private_dataset() {
    let private_dataset_handle = odf::metadata::testing::handle(&"owner", &"private-dataset");

    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    harness
        .create_private_datasets(&[&private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = private_dataset_handle.id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_read_but_not_write_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;

    harness
        .create_public_datasets(&[&not_owned_public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_not_read_and_write_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;

    harness
        .create_private_datasets(&[&not_owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_handle.id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged_admin()).await;

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
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset");

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged_admin()).await;

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
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
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

    let alice_private_dataset_1_handle = handle(&"alice", &"private-dataset-1");
    let alice_public_dataset_2_handle = handle(&"alice", &"public-dataset-2");
    let bob_private_dataset_3_handle = handle(&"bob", &"private-dataset-3");
    let bob_public_dataset_4_handle = handle(&"bob", &"public-dataset-4");

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
        .map(|h| h.id.clone())
        .collect::<Vec<_>>();

    let subjects_with_expected_results = [
        (
            CurrentAccountSubjectTestHelper::anonymous(),
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
                    - bob/private-dataset-3: Forbidden
                    - alice/private-dataset-1: Forbidden
                    "#
                ),
                write_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:

                    unauthorized_with_errors:
                    - alice/public-dataset-2: Forbidden
                    - bob/public-dataset-4: Forbidden
                    - bob/private-dataset-3: Forbidden
                    - alice/private-dataset-1: Forbidden
                    "#
                ),
                read_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Forbidden
                    - alice/private-dataset-1: Forbidden
                    "#
                ),
                write_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:

                    unauthorized_with_errors:
                    - alice/public-dataset-2: Forbidden
                    - bob/public-dataset-4: Forbidden
                    - bob/private-dataset-3: Forbidden
                    - alice/private-dataset-1: Forbidden
                    "#
                ),
            },
        ),
        (
            CurrentAccountSubjectTestHelper::logged("alice"),
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
                    - bob/private-dataset-3: Forbidden
                    "#
                ),
                write_classify_dataset_handles_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/public-dataset-4: Forbidden
                    - bob/private-dataset-3: Forbidden
                    "#
                ),
                read_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - bob/public-dataset-4
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/private-dataset-3: Forbidden
                    "#
                ),
                write_classify_dataset_ids_by_allowance_result: indoc::indoc!(
                    r#"
                    authorized:
                    - alice/public-dataset-2
                    - alice/private-dataset-1

                    unauthorized_with_errors:
                    - bob/public-dataset-4: Forbidden
                    - bob/private-dataset-3: Forbidden
                    "#
                ),
            },
        ),
        (
            CurrentAccountSubjectTestHelper::logged_admin(),
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

    for (subject, expected_results) in subjects_with_expected_results {
        let harness = DatasetAuthorizerHarness::new(subject).await;

        harness
            .create_private_datasets(&[
                &alice_private_dataset_1_handle,
                &bob_private_dataset_3_handle,
            ])
            .await;
        harness
            .create_public_datasets(&[&alice_public_dataset_2_handle, &bob_public_dataset_4_handle])
            .await;

        pretty_assertions::assert_eq!(
            {
                let mut expected = expected_results.read_filter_datasets_allowing_result;
                expected.sort_by(|left, right| left.id.cmp(&right.id));
                expected
            },
            {
                let mut actual = harness
                    .dataset_authorizer
                    .filter_datasets_allowing(all_dataset_handles.clone(), DatasetAction::Read)
                    .await
                    .unwrap();
                actual.sort_by(|left, right| left.id.cmp(&right.id));
                actual
            }
        );
        pretty_assertions::assert_eq!(
            {
                let mut expected = expected_results.write_filter_datasets_allowing_result;
                expected.sort_by(|left, right| left.id.cmp(&right.id));
                expected
            },
            {
                let mut res = harness
                    .dataset_authorizer
                    .filter_datasets_allowing(all_dataset_handles.clone(), DatasetAction::Write)
                    .await
                    .unwrap();
                res.sort_by(|left, right| left.id.cmp(&right.id));
                res
            }
        );

        pretty_assertions::assert_eq!(
            expected_results.read_classify_dataset_handles_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_authorizer
                    .classify_dataset_handles_by_allowance(
                        all_dataset_handles.clone(),
                        DatasetAction::Read
                    )
                    .await
                    .unwrap()
                    .into(),
                &dataset_handle_map
            ),
        );
        pretty_assertions::assert_eq!(
            expected_results.write_classify_dataset_handles_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_authorizer
                    .classify_dataset_handles_by_allowance(
                        all_dataset_handles.clone(),
                        DatasetAction::Write
                    )
                    .await
                    .unwrap()
                    .into(),
                &dataset_handle_map
            ),
        );

        pretty_assertions::assert_eq!(
            expected_results.read_classify_dataset_ids_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_authorizer
                    .classify_dataset_ids_by_allowance(all_dataset_ids.clone(), DatasetAction::Read)
                    .await
                    .unwrap(),
                &dataset_handle_map
            ),
        );
        pretty_assertions::assert_eq!(
            expected_results.write_classify_dataset_ids_by_allowance_result,
            ClassifyByAllowanceIdsResponseTestHelper::report(
                harness
                    .dataset_authorizer
                    .classify_dataset_ids_by_allowance(
                        all_dataset_ids.clone(),
                        DatasetAction::Write
                    )
                    .await
                    .unwrap(),
                &dataset_handle_map
            ),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetAuthorizerHarness {
    dataset_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl DatasetAuthorizerHarness {
    pub async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();

        if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
            let mut account_config =
                AccountConfig::test_config_from_name(logged_account.account_name.clone());
            account_config.is_admin = logged_account.is_admin;

            predefined_accounts_config.predefined.push(account_config);
        }

        let catalog = {
            let tenancy_config = TenancyConfig::MultiTenant;

            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add_value(current_account_subject)
                .add_value(predefined_accounts_config)
                .add::<PredefinedAccountsRegistrator>()
                .add::<kamu_auth_rebac_services::RebacServiceImpl>()
                .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
                .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                    allows_anonymous_read: false,
                    allows_public_read: false,
                })
                .add::<kamu_auth_rebac_services::RebacDatasetLifecycleMessageConsumer>()
                .add::<InMemoryRebacRepository>()
                .add_builder(
                    OutboxImmediateImpl::builder()
                        .with_consumer_filter(ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add_value(odf::dataset::MockDatasetStorageUnit::new())
                .bind::<dyn odf::DatasetStorageUnit, odf::dataset::MockDatasetStorageUnit>()
                .add_value(tenancy_config)
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<LoginPasswordAuthProvider>();

            kamu_adapter_auth_oso_rebac::register_dependencies(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            b.build()
        };

        {
            use init_on_startup::InitOnStartup;
            catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .unwrap()
                .run_initialization()
                .await
                .unwrap();
        };

        Self {
            dataset_authorizer: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            dataset_entry_writer: catalog.get_one().unwrap(),
        }
    }

    async fn create_public_datasets(&self, datasets_handles: &[&odf::DatasetHandle]) {
        self.create_dataset(datasets_handles, odf::DatasetVisibility::Public)
            .await;
    }

    async fn create_private_datasets(&self, datasets_handles: &[&odf::DatasetHandle]) {
        self.create_dataset(datasets_handles, odf::DatasetVisibility::Private)
            .await;
    }

    async fn create_dataset(
        &self,
        datasets_handles: &[&odf::DatasetHandle],
        visibility: odf::DatasetVisibility,
    ) {
        for dataset_handle in datasets_handles {
            let account_id = account_id(&dataset_handle.alias);

            self.dataset_entry_writer
                .create_entry(
                    &dataset_handle.id,
                    &account_id,
                    &dataset_handle.alias.dataset_name,
                )
                .await
                .unwrap();

            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                    DatasetLifecycleMessage::created(
                        dataset_handle.id.clone(),
                        account_id,
                        visibility,
                        dataset_handle.alias.dataset_name.clone(),
                    ),
                )
                .await
                .unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn account_id(alias: &odf::DatasetAlias) -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519(alias.account_name.as_ref().unwrap().as_bytes())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
