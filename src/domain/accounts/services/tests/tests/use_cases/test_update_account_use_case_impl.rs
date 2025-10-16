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

use bon::bon;
use database_common::NoOpDatabasePlugin;
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::utils::{AccountAuthorizationHelper, MockAccountAuthorizationHelper};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    UpdateAccountUseCaseImpl,
};
use messaging_outbox::{MockOutbox, Outbox};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ADMIN: &str = "admin";
const REGULAR_USER: &str = "regular-user";
const NEW_REGULAR_USER_NAME: &str = "regular-user-new";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_account_email_success() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .mock_account_authorization_helper(MockAccountAuthorizationHelper::allowing())
        .mock_outbox(mock_outbox)
        .build()
        .await;

    let account_name = odf::AccountName::new_unchecked("foo");
    let created_account = harness.create_account(&account_name).await;

    let new_email = email_utils::Email::parse("foo@example.com").unwrap();
    let account_to_update = Account {
        email: new_email.clone(),
        ..created_account.clone()
    };

    harness
        .update_use_case
        .execute(&account_to_update)
        .await
        .unwrap();

    let account = harness
        .account_service
        .get_account_by_id(&created_account.id)
        .await
        .unwrap();

    assert_eq!(account.email, new_email);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_account_email_duplicate_error() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .mock_account_authorization_helper(MockAccountAuthorizationHelper::allowing())
        .mock_outbox(mock_outbox)
        .build()
        .await;

    let account_name_foo = odf::AccountName::new_unchecked("foo");
    let account_name_bar = odf::AccountName::new_unchecked("bar");
    let created_account_foo = harness.create_account(&account_name_foo).await;
    let created_account_bar = harness.create_account(&account_name_bar).await;
    let account_to_update = Account {
        email: created_account_bar.email.clone(),
        ..created_account_foo.clone()
    };

    // Try to modify email from existing account

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Err(UpdateAccountError::Duplicate(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rename_own_account() {
    let mut outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_renamed(&mut outbox);

    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                .set_properties(vec![AccountPropertyName::IsAdmin]),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
        ];
        p
    };

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .predefined_account_config(predefined_account_config)
        .current_account_subject(CurrentAccountSubject::new_test_with(&REGULAR_USER))
        .mock_outbox(outbox)
        .build()
        .await;

    let mut account_to_update = harness.find_account_by_name(&REGULAR_USER).await.unwrap();
    account_to_update.account_name = odf::AccountName::new_unchecked(NEW_REGULAR_USER_NAME);

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Ok(())
    );

    assert_matches!(
        harness.find_account_by_name(&NEW_REGULAR_USER_NAME).await,
        Some(account)
            if account.account_name == odf::AccountName::new_unchecked(NEW_REGULAR_USER_NAME)
    );

    assert_matches!(harness.find_account_by_name(&REGULAR_USER).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_name() {
    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                .set_properties(vec![AccountPropertyName::IsAdmin]),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
        ];
        p
    };

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .predefined_account_config(predefined_account_config)
        .current_account_subject(CurrentAccountSubject::new_test_with(&REGULAR_USER))
        .mock_outbox(MockOutbox::new())
        .build()
        .await;

    let mut account_to_update = harness.find_account_by_name(&REGULAR_USER).await.unwrap();
    account_to_update.account_name = odf::AccountName::new_unchecked(ADMIN);

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Err(UpdateAccountError::Duplicate(AccountErrorDuplicate {
            account_field: AccountDuplicateField::Name
        }))
    );

    account_to_update.account_name = odf::AccountName::new_unchecked(REGULAR_USER);

    // Rename to self is fine, idempontence
    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_renames_other_account() {
    let mut outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_renamed(&mut outbox);

    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                .set_properties(vec![AccountPropertyName::IsAdmin]),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
        ];
        p
    };

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .predefined_account_config(predefined_account_config)
        .current_account_subject(CurrentAccountSubject::new_test_with(&ADMIN))
        .mock_outbox(outbox)
        .build()
        .await;

    let mut account_to_update = harness.find_account_by_name(&REGULAR_USER).await.unwrap();
    account_to_update.account_name = odf::AccountName::new_unchecked(NEW_REGULAR_USER_NAME);

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Ok(())
    );

    assert_matches!(
        harness.find_account_by_name(&NEW_REGULAR_USER_NAME).await,
        Some(account)
            if account.account_name == odf::AccountName::new_unchecked(NEW_REGULAR_USER_NAME)
    );

    assert_matches!(harness.find_account_by_name(&REGULAR_USER).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_try_to_rename_account() {
    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                .set_properties(vec![AccountPropertyName::IsAdmin]),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
        ];
        p
    };

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .predefined_account_config(predefined_account_config)
        .current_account_subject(CurrentAccountSubject::anonymous(
            AnonymousAccountReason::NoAuthenticationProvided,
        ))
        .mock_outbox(MockOutbox::new())
        .build()
        .await;

    let mut account_to_update = harness.find_account_by_name(&REGULAR_USER).await.unwrap();
    account_to_update.account_name = odf::AccountName::new_unchecked(NEW_REGULAR_USER_NAME);

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Err(UpdateAccountError::Access(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_rename_other_account() {
    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                .set_properties(vec![AccountPropertyName::IsAdmin]),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
        ];
        p
    };

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .predefined_account_config(predefined_account_config)
        .current_account_subject(CurrentAccountSubject::new_test_with(&REGULAR_USER))
        .mock_outbox(MockOutbox::new())
        .build()
        .await;

    let mut account_to_update = harness.find_account_by_name(&ADMIN).await.unwrap();
    account_to_update.account_name = odf::AccountName::new_unchecked("admin-new");

    assert_matches!(
        harness.update_use_case.execute(&account_to_update).await,
        Err(UpdateAccountError::Access(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct UpdateAccountUseCaseImplHarness {
    create_use_case: Arc<dyn CreateAccountUseCase>,
    update_use_case: Arc<dyn UpdateAccountUseCase>,
    account_service: Arc<dyn AccountService>,
}

#[bon]
impl UpdateAccountUseCaseImplHarness {
    #[builder]
    async fn new(
        mock_account_authorization_helper: Option<MockAccountAuthorizationHelper>,
        mock_outbox: MockOutbox,
        current_account_subject: Option<CurrentAccountSubject>,
        predefined_account_config: Option<PredefinedAccountsConfig>,
    ) -> Self {
        let mut b = dill::CatalogBuilder::new();

        b.add::<CreateAccountUseCaseImpl>();
        b.add::<AccountServiceImpl>();
        b.add::<InMemoryAccountRepository>();
        b.add_value(DidSecretEncryptionConfig::sample());
        b.add::<InMemoryDidSecretKeyRepository>();
        b.add::<SystemTimeSourceDefault>();
        b.add_value(mock_outbox);
        b.bind::<dyn Outbox, MockOutbox>();

        b.add::<UpdateAccountUseCaseImpl>();
        if let Some(current_account_subject) = current_account_subject {
            b.add_value(current_account_subject);
        }
        if let Some(predefined_account_config) = predefined_account_config {
            b.add::<kamu_accounts_services::PredefinedAccountsRegistrator>();
            b.add::<kamu_accounts_services::LoginPasswordAuthProvider>();
            b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();
            b.add::<kamu_auth_rebac_services::RebacServiceImpl>();
            b.add_value(kamu_auth_rebac_services::DefaultAccountProperties::default());
            b.add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default());
            b.add_value(predefined_account_config);
        }
        if let Some(mock_account_authorization_helper) = mock_account_authorization_helper {
            b.add_value(mock_account_authorization_helper)
                .bind::<dyn AccountAuthorizationHelper, MockAccountAuthorizationHelper>();
        } else {
            b.add::<kamu_accounts_services::utils::AccountAuthorizationHelperImpl>();
        }

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            create_use_case: catalog.get_one().unwrap(),
            update_use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }

    async fn create_account(&self, account_name: &odf::AccountName) -> Account {
        let creator_account = Account::dummy();

        self.create_use_case
            .execute_derived(
                &creator_account,
                account_name,
                CreateAccountUseCaseOptions::default(),
            )
            .await
            .unwrap()
    }

    pub fn expect_outbox_account_created(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{always, eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone()),
                        Ok(AccountLifecycleMessage::Created(_))
                    )
                }),
                always(),
            )
            .returning(|_, _, _| Ok(()));
    }

    fn expect_outbox_account_renamed(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    let message_res =
                        serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone())
                            .unwrap();
                    matches!(message_res, AccountLifecycleMessage::Renamed(_))
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }

    async fn find_account_by_name(&self, account_name: &impl AsRef<str>) -> Option<Account> {
        self.account_service
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
