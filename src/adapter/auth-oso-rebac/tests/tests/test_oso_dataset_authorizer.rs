// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use dill::Component;
use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{
    AccountConfig,
    AccountPropertyName,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_NAME,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_core::TenancyConfig;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::testing::ClassifyByAllowanceIdsResponseTestHelper;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_services::{DatasetEntryServiceImpl, DatasetEntryWriter};
use messaging_outbox::{
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
    register_message_dispatcher,
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

// TODO: Private Datasets: cover new paths

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_private_dataset() {
    let owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner"), false)
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
                    DatasetAction::Read,
                    DatasetAction::Write,
                    DatasetAction::Maintain,
                    DatasetAction::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_public_dataset() {
    let owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner"), false)
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
                    DatasetAction::Read,
                    DatasetAction::Write,
                    DatasetAction::Maintain,
                    DatasetAction::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write_public_dataset() {
    let public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous(), false).await;
    harness
        .create_public_datasets(&[&public_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = public_dataset_handle.id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_not_read_and_write_private_dataset() {
    let private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous(), false).await;
    harness
        .create_private_datasets(&[&private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = private_dataset_handle.id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_read_but_not_write_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner"), false)
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
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_not_read_and_write_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner"), false)
            .await;

    harness
        .create_private_datasets(&[&not_owned_private_dataset_handle])
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_handle.id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Unauthorized(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_public_dataset() {
    let not_owned_public_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"public-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("admin"), true).await;

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
                    DatasetAction::Read,
                    DatasetAction::Write,
                    DatasetAction::Maintain,
                    DatasetAction::Own
                ].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_private_dataset() {
    let not_owned_private_dataset_handle =
        odf::metadata::testing::handle(&"owner", &"private-dataset", odf::DatasetKind::Root);

    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("admin"), true).await;

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
                    DatasetAction::Read,
                    DatasetAction::Write,
                    DatasetAction::Maintain,
                    DatasetAction::Own
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
        let harness = DatasetAuthorizerHarness::new(subject, is_admin).await;

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
                    .classify_dataset_ids_by_allowance(&all_dataset_ids, DatasetAction::Read)
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
                    .classify_dataset_ids_by_allowance(&all_dataset_ids, DatasetAction::Write)
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
    pub async fn new(current_account_subject: CurrentAccountSubject, is_admin: bool) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();

        if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
            let mut account_properties = Vec::new();
            if is_admin {
                account_properties.push(AccountPropertyName::IsAdmin);
            }
            let account_config =
                AccountConfig::test_config_from_name(logged_account.account_name.clone())
                    .set_properties(account_properties);

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
                .add_value(kamu_auth_rebac_services::DefaultAccountProperties::default())
                .add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default())
                .add::<kamu_auth_rebac_services::RebacDatasetLifecycleMessageConsumer>()
                .add::<UpdateAccountUseCaseImpl>()
                .add::<CreateAccountUseCaseImpl>()
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
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(kamu_accounts::DidSecretEncryptionConfig::sample())
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
            let account_name = dataset_handle
                .alias
                .account_name
                .as_ref()
                .unwrap_or(&DEFAULT_ACCOUNT_NAME);

            self.dataset_entry_writer
                .create_entry(
                    &dataset_handle.id,
                    &account_id,
                    account_name,
                    &dataset_handle.alias.dataset_name,
                    odf::DatasetKind::Root,
                )
                .await
                .unwrap();

            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                    DatasetLifecycleMessage::created(
                        Utc::now(),
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
