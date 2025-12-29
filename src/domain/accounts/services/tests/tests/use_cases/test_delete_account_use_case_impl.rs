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

use kamu_accounts::*;
use messaging_outbox::{MockOutbox, Outbox};

use crate::tests::use_cases::{AccountBaseUseCaseHarness, AccountBaseUseCaseHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ADMIN: &str = "admin";
const REGULAR_USER: &str = "regular_user";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_own_account() {
    let mut outbox = MockOutbox::new();
    DeleteAccountUseCaseImplHarness::expect_outbox_account_deleted(&mut outbox);

    let harness = DeleteAccountUseCaseImplHarness::new(
        CurrentAccountSubject::new_test_with(&REGULAR_USER),
        outbox,
    )
    .await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness
            .delete_account_use_case
            .execute(&regular_user_account)
            .await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness
            .delete_account_use_case
            .execute(&regular_user_account)
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_delete_other_account() {
    let mut outbox = MockOutbox::new();
    DeleteAccountUseCaseImplHarness::expect_outbox_account_deleted(&mut outbox);

    let harness =
        DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::new_test_with(&ADMIN), outbox)
            .await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness
            .delete_account_use_case
            .execute(&regular_user_account)
            .await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness
            .delete_account_use_case
            .execute(&regular_user_account)
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_try_to_delete_account() {
    let harness = DeleteAccountUseCaseImplHarness::new(
        CurrentAccountSubject::anonymous(AnonymousAccountReason::NoAuthenticationProvided),
        MockOutbox::new(),
    )
    .await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness.delete_account_use_case.execute(&regular_user_account).await,
        Err(DeleteAccountError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == format!("Anonymous is not authorized to delete account '{REGULAR_USER}'")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_delete_other_account() {
    let harness = DeleteAccountUseCaseImplHarness::new(
        CurrentAccountSubject::new_test_with(&REGULAR_USER),
        MockOutbox::new(),
    )
    .await;

    let admin_user_account = harness.get_account_by_name(&ADMIN).await;

    assert_matches!(
        harness.delete_account_use_case.execute(&admin_user_account).await,
        Err(DeleteAccountError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == format!("Account '{REGULAR_USER}' is not authorized to delete account '{ADMIN}'")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(AccountBaseUseCaseHarness, account_base_harness)]
struct DeleteAccountUseCaseImplHarness {
    account_base_harness: AccountBaseUseCaseHarness,
    delete_account_use_case: Arc<dyn DeleteAccountUseCase>,
}

impl DeleteAccountUseCaseImplHarness {
    async fn new(current_account_subject: CurrentAccountSubject, outbox: MockOutbox) -> Self {
        let predefined_account_config = {
            let mut p = PredefinedAccountsConfig::new();
            p.predefined = vec![
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                    .set_properties(vec![AccountPropertyName::IsAdmin]),
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
            ];
            p
        };

        let account_base_harness = AccountBaseUseCaseHarness::new(AccountBaseUseCaseHarnessOpts {
            maybe_predefined_accounts_config: Some(predefined_account_config),
            ..Default::default()
        });

        let mut b = dill::CatalogBuilder::new_chained(account_base_harness.intermediate_catalog());

        b.add_value(outbox);
        b.bind::<dyn Outbox, MockOutbox>();
        b.add_value(current_account_subject);

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            account_base_harness,
            delete_account_use_case: catalog.get_one().unwrap(),
        }
    }

    fn expect_outbox_account_deleted(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    let message_res =
                        serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone())
                            .unwrap();
                    matches!(message_res, AccountLifecycleMessage::Deleted(_))
                }),
                eq(2),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
