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

use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    AccountService,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    DidSecretEncryptionConfig,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    UpdateAccountEmailError,
    UpdateAccountEmailUseCase,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::utils::{AccountAuthorizationHelper, MockAccountAuthorizationHelper};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    UpdateAccountEmailUseCaseImpl,
};
use messaging_outbox::{MockOutbox, Outbox};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_account_email_success() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountEmailUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);

    let harness = UpdateAccountEmailUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::allowing(),
        mock_outbox,
    )
    .await;

    let account_name = odf::AccountName::new_unchecked("foo");
    let created_account = harness.create_account(&account_name).await;

    let new_email = email_utils::Email::parse("foo@example.com").unwrap();
    harness
        .update_email_use_case
        .execute(&created_account, new_email.clone())
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
async fn test_update_account_email_duplicate() {
    let mut mock_outbox = MockOutbox::new();
    UpdateAccountEmailUseCaseImplHarness::expect_outbox_account_created(&mut mock_outbox);

    let harness = UpdateAccountEmailUseCaseImplHarness::new(
        MockAccountAuthorizationHelper::allowing(),
        mock_outbox,
    )
    .await;

    let account_name_foo = odf::AccountName::new_unchecked("foo");
    let account_name_bar = odf::AccountName::new_unchecked("bar");
    let created_account_foo = harness.create_account(&account_name_foo).await;
    let created_account_bar = harness.create_account(&account_name_bar).await;

    // Try to modify email from existing account

    assert_matches!(
        harness
            .update_email_use_case
            .execute(&created_account_foo, created_account_bar.email.clone())
            .await,
        Err(UpdateAccountEmailError::Duplicate(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct UpdateAccountEmailUseCaseImplHarness {
    create_use_case: Arc<dyn CreateAccountUseCase>,
    update_email_use_case: Arc<dyn UpdateAccountEmailUseCase>,
    account_service: Arc<dyn AccountService>,
}

impl UpdateAccountEmailUseCaseImplHarness {
    async fn new(
        mock_account_authorization_helper: MockAccountAuthorizationHelper,
        mock_outbox: MockOutbox,
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

        b.add::<UpdateAccountEmailUseCaseImpl>();
        b.add_value(mock_account_authorization_helper)
            .bind::<dyn AccountAuthorizationHelper, MockAccountAuthorizationHelper>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            create_use_case: catalog.get_one().unwrap(),
            update_email_use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }

    async fn create_account(&self, account_name: &odf::AccountName) -> Account {
        let creator_account = Account::dummy();

        self.create_use_case
            .execute(
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
