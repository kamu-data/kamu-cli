// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    Account,
    AccountService,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    DidSecretEncryptionConfig,
    ModifyAccountPasswordError,
    ModifyAccountPasswordUseCase,
    ModifyAccountPasswordWithConfirmationError,
    Password,
    TEST_PASSWORD,
    VerifyPasswordError,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::utils::{AccountAuthorizationHelper, MockAccountAuthorizationHelper};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    ModifyAccountPasswordUseCaseImpl,
};
use messaging_outbox::DummyOutboxImpl;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_success() {
    let harness =
        ModifyAccountPasswordUseCaseImplHarness::new(MockAccountAuthorizationHelper::allowing())
            .await;

    let initial_password = TEST_PASSWORD.clone();
    let account = harness.create_account(initial_password.clone()).await;

    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Ok(_),
    );

    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_use_case
            .execute(&account, new_password.clone())
            .await,
        Ok(_),
    );

    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Ok(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_not_admin() {
    let harness =
        ModifyAccountPasswordUseCaseImplHarness::new(MockAccountAuthorizationHelper::disallowing())
            .await;

    let account = harness.create_account(TEST_PASSWORD.clone()).await;
    let new_password = Password::try_new("new_password_1").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_use_case
            .execute(&account, new_password.clone())
            .await,
        Err(ModifyAccountPasswordError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == "Account 'user-without-access' is not authorized to modify account's password 'new-account'"
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_success() {
    let harness =
        ModifyAccountPasswordUseCaseImplHarness::new(MockAccountAuthorizationHelper::allowing())
            .await;

    let initial_password = TEST_PASSWORD.clone();
    let account = harness.create_account(initial_password.clone()).await;
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_use_case
            .execute_with_confirmation(&account, initial_password.clone(), new_password.clone())
            .await,
        Ok(_),
    );

    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Ok(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_incorrect_old_password() {
    let harness =
        ModifyAccountPasswordUseCaseImplHarness::new(MockAccountAuthorizationHelper::allowing())
            .await;

    let initial_password = TEST_PASSWORD.clone();
    let account = harness.create_account(initial_password).await;

    let wrong_old_password = Password::try_new("wrong-old-password").unwrap();
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_use_case
            .execute_with_confirmation(&account, wrong_old_password, new_password)
            .await,
        Err(ModifyAccountPasswordWithConfirmationError::WrongOldPassword(_)),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_incorrect_not_enough_permissions() {
    let harness =
        ModifyAccountPasswordUseCaseImplHarness::new(MockAccountAuthorizationHelper::disallowing())
            .await;

    let initial_password = TEST_PASSWORD.clone();
    let account = harness.create_account(initial_password.clone()).await;
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_use_case
            .execute_with_confirmation(&account, initial_password, new_password)
            .await,
        Err(ModifyAccountPasswordWithConfirmationError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == "Account 'user-without-access' is not authorized to modify account's password 'new-account'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ModifyAccountPasswordUseCaseImplHarness {
    create_use_case: Arc<dyn CreateAccountUseCase>,
    modify_use_case: Arc<dyn ModifyAccountPasswordUseCase>,
    account_service: Arc<dyn AccountService>,
}

impl ModifyAccountPasswordUseCaseImplHarness {
    async fn new(mock_account_authorization_helper: MockAccountAuthorizationHelper) -> Self {
        let mut b = dill::CatalogBuilder::new();

        b.add::<CreateAccountUseCaseImpl>();
        b.add::<AccountServiceImpl>();
        b.add::<DummyOutboxImpl>();
        b.add::<InMemoryAccountRepository>();
        b.add_value(DidSecretEncryptionConfig::sample());
        b.add::<InMemoryDidSecretKeyRepository>();
        b.add::<SystemTimeSourceDefault>();

        b.add::<ModifyAccountPasswordUseCaseImpl>();
        b.add_value(mock_account_authorization_helper)
            .bind::<dyn AccountAuthorizationHelper, MockAccountAuthorizationHelper>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            create_use_case: catalog.get_one().unwrap(),
            modify_use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }

    async fn create_account(&self, password: Password) -> Account {
        let creator_account = Account::dummy();
        let account_name = odf::AccountName::new_unchecked("new-account");

        self.create_use_case
            .execute(
                &creator_account,
                &account_name,
                CreateAccountUseCaseOptions::builder()
                    .password(password)
                    .build(),
            )
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
