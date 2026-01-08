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
use kamu_accounts_services::utils::{
    AccountAuthorizationHelperTestProvider,
    MockAccountAuthorizationHelper,
};
use messaging_outbox::{MockOutbox, Outbox};

use crate::tests::use_cases::{AccountBaseUseCaseHarness, AccountBaseUseCaseHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ADMIN: &str = "admin";
const REGULAR_USER: &str = "regular-user";
const NEW_REGULAR_USER_NAME: &str = "regular-user-new";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_account_email_success() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);
    UpdateAccountUseCaseImplHarness::expect_outbox_account_email_changed(&mut mock_outbox);

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .account_authorization_helper_provider(AccountAuthorizationHelperTestProvider::Mock(
            MockAccountAuthorizationHelper::allowing(),
        ))
        .mock_outbox(mock_outbox)
        .build()
        .await;

    let created_account = harness.create_account(&harness.catalog, "foo").await;

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

    let account = harness.get_account_by_id(&created_account.id).await;
    assert_eq!(account.email, new_email);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_account_email_duplicate_error() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .account_authorization_helper_provider(AccountAuthorizationHelperTestProvider::Mock(
            MockAccountAuthorizationHelper::allowing(),
        ))
        .mock_outbox(mock_outbox)
        .build()
        .await;

    let created_account_foo = harness.create_account(&harness.catalog, "foo").await;
    let created_account_bar = harness.create_account(&harness.catalog, "bar").await;
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
async fn test_update_account_display_name_success() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);
    UpdateAccountUseCaseImplHarness::expect_outbox_account_display_name_changed(&mut mock_outbox);

    let harness = UpdateAccountUseCaseImplHarness::builder()
        .account_authorization_helper_provider(AccountAuthorizationHelperTestProvider::Mock(
            MockAccountAuthorizationHelper::allowing(),
        ))
        .mock_outbox(mock_outbox)
        .build()
        .await;

    let created_account = harness.create_account(&harness.catalog, "foo").await;

    let new_display_name = AccountDisplayName::from("Foo Bar");
    let account_to_update = Account {
        display_name: new_display_name.clone(),
        ..created_account.clone()
    };

    harness
        .update_use_case
        .execute(&account_to_update)
        .await
        .unwrap();

    let account = harness.get_account_by_id(&created_account.id).await;

    assert_eq!(account.display_name, new_display_name);
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
        .account_authorization_helper_provider(Default::default())
        .maybe_predefined_accounts_config(predefined_account_config)
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
        .account_authorization_helper_provider(Default::default())
        .maybe_predefined_accounts_config(predefined_account_config)
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
        .account_authorization_helper_provider(Default::default())
        .maybe_predefined_accounts_config(predefined_account_config)
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
        .account_authorization_helper_provider(Default::default())
        .maybe_predefined_accounts_config(predefined_account_config)
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
        .account_authorization_helper_provider(Default::default())
        .maybe_predefined_accounts_config(predefined_account_config)
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

#[oop::extend(AccountBaseUseCaseHarness, account_base_harness)]
struct UpdateAccountUseCaseImplHarness {
    account_base_harness: AccountBaseUseCaseHarness,
    update_use_case: Arc<dyn UpdateAccountUseCase>,
    catalog: dill::Catalog,
}

#[bon]
impl UpdateAccountUseCaseImplHarness {
    #[builder]
    async fn new(
        account_authorization_helper_provider: AccountAuthorizationHelperTestProvider,
        mock_outbox: MockOutbox,
        current_account_subject: Option<CurrentAccountSubject>,
        maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
    ) -> Self {
        let account_base_harness = AccountBaseUseCaseHarness::new(AccountBaseUseCaseHarnessOpts {
            account_authorization_helper_provider,
            maybe_predefined_accounts_config,
            ..Default::default()
        });

        let mut b = dill::CatalogBuilder::new_chained(account_base_harness.intermediate_catalog());
        b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();

        if let Some(current_account_subject) = current_account_subject {
            b.add_value(current_account_subject);
        }

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            account_base_harness,
            update_use_case: catalog.get_one().unwrap(),
            catalog,
        }
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

    fn expect_outbox_account_email_changed(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    let message_res =
                        serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone())
                            .unwrap();
                    matches!(message_res, AccountLifecycleMessage::Updated(u)
                        if u.old_email != u.new_email)
                }),
                eq(2),
            )
            .returning(|_, _, _| Ok(()));
    }

    fn expect_outbox_account_display_name_changed(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    let message_res =
                        serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone())
                            .unwrap();
                    matches!(message_res, AccountLifecycleMessage::Updated(u)
                        if u.old_display_name != u.new_display_name)
                }),
                eq(2),
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
                    matches!(message_res, AccountLifecycleMessage::Updated(u)
                        if u.old_account_name != u.new_account_name)
                }),
                eq(2),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
