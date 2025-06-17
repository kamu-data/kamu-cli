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
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
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
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
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
        harness.use_case.execute(&regular_user_account).await,
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
        harness.use_case.execute(&admin_user_account).await,
        Err(DeleteAccountError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == format!("Account '{REGULAR_USER}' is not authorized to delete account '{ADMIN}'")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteAccountUseCaseImplHarness {
    use_case: Arc<dyn DeleteAccountUseCase>,
    account_service: Arc<dyn AccountService>,
}

impl DeleteAccountUseCaseImplHarness {
    async fn new(current_account_subject: CurrentAccountSubject, outbox: MockOutbox) -> Self {
        let mut b = dill::CatalogBuilder::new();

        let predefined_account_config = {
            let mut p = PredefinedAccountsConfig::new();
            p.predefined = vec![
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                    .set_properties(vec![kamu_auth_rebac::AccountPropertyName::IsAdmin]),
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
            ];
            p
        };

        b.add::<kamu_accounts_inmem::InMemoryAccountRepository>();
        b.add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>();
        b.add::<kamu_accounts_services::AccountServiceImpl>();
        b.add::<kamu_accounts_services::DeleteAccountUseCaseImpl>();
        b.add::<kamu_accounts_services::LoginPasswordAuthProvider>();
        b.add::<kamu_accounts_services::PredefinedAccountsRegistrator>();
        b.add::<kamu_accounts_services::utils::AccountAuthorizationHelperImpl>();
        b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();
        b.add::<kamu_auth_rebac_services::RebacServiceImpl>();
        b.add_value(outbox);
        b.bind::<dyn Outbox, MockOutbox>();
        b.add::<time_source::SystemTimeSourceDefault>();
        b.add_value(DidSecretEncryptionConfig::sample());
        b.add_value(current_account_subject);
        b.add_value(kamu_auth_rebac_services::DefaultAccountProperties::default());
        b.add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default());
        b.add_value(predefined_account_config);

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }

    async fn get_account_by_name(&self, account_name: &impl AsRef<str>) -> Account {
        self.account_service
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .unwrap()
    }

    async fn account_exists(&self, account_name: &impl AsRef<str>) -> bool {
        self.account_service
            .find_account_id_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .is_some()
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
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
