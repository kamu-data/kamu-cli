// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ModifyAccountPasswordError,
    ModifyAccountPasswordUseCase,
    ModifyAccountPasswordWithConfirmationError,
    Password,
    TEST_PASSWORD,
    VerifyPasswordError,
};
use kamu_accounts_services::ModifyAccountPasswordUseCaseImpl;
use kamu_accounts_services::utils::{
    AccountAuthorizationHelperTestProvider,
    MockAccountAuthorizationHelper,
};
use messaging_outbox::{MockOutbox, Outbox};

use crate::tests::use_cases::{AccountBaseUseCaseHarness, AccountBaseUseCaseHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_success() {
    let mut outbox = MockOutbox::new();
    ModifyAccountPasswordUseCaseImplHarness::expect_outbox_account_messages(&mut outbox);

    let harness = ModifyAccountPasswordUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::allowing(),
        outbox,
    );

    let initial_password = TEST_PASSWORD.clone();
    let account = harness
        .create_account_with_password(initial_password.clone())
        .await;

    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Ok(_),
    );

    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_account_password_use_case
            .execute(&account, new_password.clone())
            .await,
        Ok(_),
    );

    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Ok(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_not_admin() {
    let mut outbox = MockOutbox::new();
    ModifyAccountPasswordUseCaseImplHarness::expect_outbox_account_messages(&mut outbox);

    let harness = ModifyAccountPasswordUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::disallowing(),
        outbox,
    );

    let account = harness
        .create_account_with_password(TEST_PASSWORD.clone())
        .await;
    let new_password = Password::try_new("new_password_1").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_account_password_use_case
            .execute(&account, new_password.clone())
            .await,
        Err(ModifyAccountPasswordError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == "Account 'user-without-access' is not authorized to modify account's password 'new-account'"
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_success() {
    let mut outbox = MockOutbox::new();
    ModifyAccountPasswordUseCaseImplHarness::expect_outbox_account_messages(&mut outbox);

    let harness = ModifyAccountPasswordUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::allowing(),
        outbox,
    );

    let initial_password = TEST_PASSWORD.clone();
    let account = harness
        .create_account_with_password(initial_password.clone())
        .await;
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_account_password_use_case
            .execute_with_confirmation(&account, initial_password.clone(), new_password.clone())
            .await,
        Ok(_),
    );

    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &initial_password)
            .await,
        Err(VerifyPasswordError::IncorrectPassword(_)),
    );
    pretty_assertions::assert_matches!(
        harness
            .account_service()
            .verify_account_password(&account.account_name, &new_password)
            .await,
        Ok(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_incorrect_old_password() {
    let mut outbox = MockOutbox::new();
    ModifyAccountPasswordUseCaseImplHarness::expect_outbox_account_messages(&mut outbox);

    let harness = ModifyAccountPasswordUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::allowing(),
        outbox,
    );

    let initial_password = TEST_PASSWORD.clone();
    let account = harness.create_account_with_password(initial_password).await;

    let wrong_old_password = Password::try_new("wrong-old-password").unwrap();
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_account_password_use_case
            .execute_with_confirmation(&account, wrong_old_password, new_password)
            .await,
        Err(ModifyAccountPasswordWithConfirmationError::WrongOldPassword(_)),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_with_confirmation_incorrect_not_enough_permissions() {
    let mut outbox = MockOutbox::new();
    ModifyAccountPasswordUseCaseImplHarness::expect_outbox_account_messages(&mut outbox);

    let harness = ModifyAccountPasswordUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::disallowing(),
        outbox,
    );

    let initial_password = TEST_PASSWORD.clone();
    let account = harness
        .create_account_with_password(initial_password.clone())
        .await;
    let new_password = Password::try_new("new_password").unwrap();

    pretty_assertions::assert_matches!(
        harness
            .modify_account_password_use_case
            .execute_with_confirmation(&account, initial_password, new_password)
            .await,
        Err(ModifyAccountPasswordWithConfirmationError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == "Account 'user-without-access' is not authorized to modify account's password with confirmation 'new-account'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(AccountBaseUseCaseHarness, account_base_harness)]
struct ModifyAccountPasswordUseCaseImplHarness {
    account_base_harness: AccountBaseUseCaseHarness,
    create_account_use_case: Arc<dyn CreateAccountUseCase>,
    modify_account_password_use_case: Arc<dyn ModifyAccountPasswordUseCase>,
}

impl ModifyAccountPasswordUseCaseImplHarness {
    fn new(
        mock_account_authorization_helper: MockAccountAuthorizationHelper,
        mock_outbox: MockOutbox,
    ) -> Self {
        let account_base_harness = AccountBaseUseCaseHarness::new(AccountBaseUseCaseHarnessOpts {
            account_authorization_helper_provider: AccountAuthorizationHelperTestProvider::Mock(
                mock_account_authorization_helper,
            ),
            ..Default::default()
        });

        let mut b = dill::CatalogBuilder::new_chained(account_base_harness.intermediate_catalog());
        b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
        b.add::<ModifyAccountPasswordUseCaseImpl>();

        let catalog = b.build();

        Self {
            account_base_harness,
            create_account_use_case: catalog.get_one().unwrap(),
            modify_account_password_use_case: catalog.get_one().unwrap(),
        }
    }

    async fn create_account_with_password(&self, password: Password) -> Account {
        let creator_account = Account::dummy();
        let account_name = odf::AccountName::new_unchecked("new-account");

        self.create_account_use_case
            .execute_derived(
                &creator_account,
                &account_name,
                CreateAccountUseCaseOptions::builder()
                    .password(password)
                    .build(),
            )
            .await
            .unwrap()
    }

    fn expect_outbox_account_messages(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{eq, function};

        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    match serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone())
                        .unwrap()
                    {
                        AccountLifecycleMessage::Created(_) => true,
                        AccountLifecycleMessage::PasswordChanged(m) => {
                            m.display_name == "new-account"
                                && m.email == "kamu+new-account@example.com".parse().unwrap()
                        }
                        AccountLifecycleMessage::Updated(_)
                        | AccountLifecycleMessage::Deleted(_) => false,
                    }
                }),
                eq(2),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
