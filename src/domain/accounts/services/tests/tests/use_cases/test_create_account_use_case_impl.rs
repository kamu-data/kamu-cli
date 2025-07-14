// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use email_utils::Email;
use kamu_accounts::{
    AccountConfig,
    AccountDisplayName,
    AccountLifecycleMessage,
    AccountService,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    DidSecretEncryptionConfig,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use messaging_outbox::{MockOutbox, Outbox};
use odf::AccountName;
use odf::metadata::DidPkh;
use pretty_assertions::assert_matches;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WASYA: &str = "wasya";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account() {
    let new_account_name_with_email = AccountName::new_unchecked("foo");
    let new_account_email = Email::parse("foo@defined.com").unwrap();

    let new_account_name_without_email = AccountName::new_unchecked("bar");
    let new_account_name_without_generated_email = Email::parse("wasya+bar@example.com").unwrap();

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_account_created_once(
        &mut mock_outbox,
        AccountDisplayName::from(new_account_name_with_email.as_str()),
        new_account_email.clone(),
    );
    expect_outbox_account_created_once(
        &mut mock_outbox,
        AccountDisplayName::from(new_account_name_without_email.as_str()),
        new_account_name_without_generated_email.clone(),
    );

    let harness = CreateAccountUseCaseImplHarness::new(mock_outbox).await;
    let creator_account_id = harness
        .account_service
        .find_account_id_by_name(&AccountName::new_unchecked(WASYA))
        .await
        .unwrap()
        .unwrap();
    let creator_account = harness
        .account_service
        .get_account_by_id(&creator_account_id)
        .await
        .unwrap();

    // Create an account with email
    assert_matches!(
        harness
            .use_case
            .execute(
                &creator_account,
                &new_account_name_with_email,
                CreateAccountUseCaseOptions::builder().email(new_account_email.clone()).build())
            .await,
        Ok(account)
            if account.email == new_account_email
                && account.account_name == new_account_name_with_email
    );

    // Create an account without email
    assert_matches!(
        harness
            .use_case
            .execute(
                &creator_account,
                &new_account_name_without_email,
                CreateAccountUseCaseOptions::default()
            )
            .await,
        Ok(account)
            if account.email == new_account_name_without_generated_email
                && account.account_name == new_account_name_without_email
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_wallet_accounts() {
    // Addresses got from: https://www.ethereumaddressgenerator.com/
    let did_pkhs = [
        "did:pkh:eip155:1:0xbf9a00755BB7d2E904b5F569095220c54E742E07",
        "did:pkh:eip155:1:0x3F41642f9813eb3be2DBc098dad815c25b9156E8",
        "did:pkh:eip155:1:0xeCd666A695086c10D8d4AB146D2827842bd15Ef9",
    ]
    .into_iter()
    .map(DidPkh::from_did_str)
    .map(Result::unwrap)
    .collect::<HashSet<_>>();

    let mut mock_outbox = MockOutbox::new();

    for did_pkh in &did_pkhs {
        // E.g. 0xbf9a00755BB7d2E904b5F569095220c54E742E07
        let wallet_address = did_pkh.wallet_address();
        expect_outbox_account_created_once(
            &mut mock_outbox,
            AccountDisplayName::from(wallet_address),
            format!("{wallet_address}@example.com").parse().unwrap(),
        );
    }

    let harness = CreateAccountUseCaseImplHarness::new(mock_outbox).await;
    let wallet_addresses_count = did_pkhs.len();

    assert_matches!(
        harness
            .use_case
            .execute_multi_wallet_accounts(did_pkhs.clone())
            .await,
        Ok(accounts)
            if accounts.len() == wallet_addresses_count
    );

    // Idempotence (but w/o new messages -- controlled by mock_outbox expectations).
    assert_matches!(
        harness
            .use_case
            .execute_multi_wallet_accounts(did_pkhs)
            .await,
        Ok(accounts)
            if accounts.len() == wallet_addresses_count
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CreateAccountUseCaseImplHarness {
    use_case: Arc<dyn CreateAccountUseCase>,
    account_service: Arc<dyn AccountService>,
}

impl CreateAccountUseCaseImplHarness {
    async fn new(mock_outbox: MockOutbox) -> Self {
        let mut b = dill::CatalogBuilder::new();

        let mut predefined_account_config = PredefinedAccountsConfig::new();
        {
            let account_name = WASYA;
            predefined_account_config
                .predefined
                .push(AccountConfig::test_config_from_name(
                    AccountName::new_unchecked(account_name),
                ));
        }

        b.add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add_value(predefined_account_config)
            .add::<SystemTimeSourceDefault>()
            .add::<LoginPasswordAuthProvider>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add::<InMemoryDidSecretKeyRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<CreateAccountUseCaseImpl>()
            .add::<PredefinedAccountsRegistrator>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn expect_outbox_account_created_once(
    mock_outbox: &mut MockOutbox,
    expected_display_name: AccountDisplayName,
    expected_email: Email,
) {
    use mockall::predicate::{always, eq, function};

    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
            function(move |message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone()),
                    Ok(AccountLifecycleMessage::Created(m))
                        if m.display_name == expected_display_name
                            && m.email == expected_email
                )
            }),
            always(),
        )
        .times(1)
        .returning(|_, _, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
