// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use internal_error::InternalError;
use kamu_accounts::{Account, AccountConfig, AccountPropertyName, PredefinedAccountsConfig};
use messaging_outbox::MockOutbox;
use pretty_assertions::{assert_eq, assert_matches};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_st_create_once_reuse_after() {
    let account_name = odf::AccountName::new_unchecked("kamu");

    let mut outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(account_name.clone())
        .expected_display_name("kamu".to_string())
        .expected_email(Email::parse("kamu@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    // 1. Creates an account (w/o ID & key)
    let catalog = harness.build_catalog(PredefinedAccountsConfig::single_tenant());

    assert_matches!(
        harness
            .try_get_account_by_name(&catalog, &account_name)
            .await,
        None
    );
    assert_matches!(harness.run_initialization(&catalog).await, Ok(_));

    let first_run_account = harness
        .try_get_account_by_name(&catalog, &account_name)
        .await
        .unwrap();
    let first_run_secret = harness
        .try_account_secret(&catalog, &first_run_account.id)
        .await
        .unwrap();

    assert_eq!(
        kamu_auth_rebac::AccountProperties {
            is_admin: true,
            can_provision_accounts: false
        },
        harness
            .try_get_account_properties(&catalog, &first_run_account.id)
            .await
    );

    // 2. Re-use the account
    assert_matches!(harness.run_initialization(&catalog).await, Ok(_));

    let second_run_account = harness
        .try_get_account_by_name(&catalog, &account_name)
        .await
        .unwrap();
    let second_run_secret = harness
        .try_account_secret(&catalog, &first_run_account.id)
        .await
        .unwrap();

    assert_eq!(first_run_account, second_run_account);
    assert_eq!(first_run_secret, second_run_secret);

    assert_eq!(
        kamu_auth_rebac::AccountProperties {
            is_admin: true,
            can_provision_accounts: false
        },
        harness
            .try_get_account_properties(&catalog, &first_run_account.id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_emulate_running_a_node_with_updated_registrator() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");
    let alice_account_id = odf::metadata::testing::account_id(&alice_account_name);
    let bob_account_name = odf::AccountName::new_unchecked("bob");
    let bob_account_id = odf::metadata::testing::account_id(&bob_account_name);

    let mut outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(alice_account_name.clone())
        .expected_display_name("alice".to_string())
        .expected_email(Email::parse("alice@example.com").unwrap())
        .expected_times(1)
        .call();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(bob_account_name.clone())
        .expected_display_name("bob".to_string())
        .expected_email(Email::parse("bob@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    // 1. Create accounts: w/ ids but w/o keys (simulate existing node accounts)
    let predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![
            AccountConfig::test_config_from_name(alice_account_name.clone())
                .set_id(Some(alice_account_id.clone())),
            AccountConfig::test_config_from_name(bob_account_name.clone())
                .set_id(Some(bob_account_id.clone())),
        ],
    };
    let catalog = harness.build_catalog(predefined_accounts_config);

    assert_matches!(
        harness
            .try_get_account_by_name(&catalog, &alice_account_name)
            .await,
        None
    );
    assert_matches!(
        harness
            .try_get_account_by_name(&catalog, &bob_account_name)
            .await,
        None
    );
    assert_matches!(harness.run_initialization(&catalog).await, Ok(_));

    // Let's make sure there are no keys
    assert_matches!(
        harness
            .try_account_secret(&catalog, &alice_account_id)
            .await,
        None
    );
    assert_matches!(
        harness.try_account_secret(&catalog, &bob_account_id).await,
        None
    );

    let first_run_alice_account = harness
        .try_get_account_by_name(&catalog, &alice_account_name)
        .await
        .unwrap();
    let first_run_bob_account = harness
        .try_get_account_by_name(&catalog, &bob_account_name)
        .await
        .unwrap();

    assert_eq!(alice_account_id, first_run_alice_account.id);
    assert_eq!(bob_account_id, first_run_bob_account.id);

    assert_eq!(
        kamu_auth_rebac_services::DefaultAccountProperties::default(),
        harness
            .try_get_account_properties(&catalog, &alice_account_id)
            .await
    );
    assert_eq!(
        kamu_auth_rebac_services::DefaultAccountProperties::default(),
        harness
            .try_get_account_properties(&catalog, &bob_account_id)
            .await
    );

    // 2. Re-use the accounts -- simulate start w/ new version
    assert_matches!(harness.run_initialization(&catalog).await, Ok(_));

    let second_run_alice_account = harness
        .try_get_account_by_name(&catalog, &alice_account_name)
        .await
        .unwrap();
    let second_run_bob_account = harness
        .try_get_account_by_name(&catalog, &bob_account_name)
        .await
        .unwrap();

    assert_eq!(first_run_alice_account, second_run_alice_account);
    assert_eq!(first_run_bob_account, second_run_bob_account);

    assert_eq!(
        kamu_auth_rebac_services::DefaultAccountProperties::default(),
        harness
            .try_get_account_properties(&catalog, &alice_account_id)
            .await
    );
    assert_eq!(
        kamu_auth_rebac_services::DefaultAccountProperties::default(),
        harness
            .try_get_account_properties(&catalog, &bob_account_id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_skip_alice_wrong_config() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");
    let alice_account_id = odf::metadata::testing::account_id(&alice_account_name);
    let bob_account_name = odf::AccountName::new_unchecked("bob");

    let mut outbox = MockOutbox::new();
    // Only Bob's account will be created
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(bob_account_name.clone())
        .expected_display_name("bob".to_string())
        .expected_email(Email::parse("bob@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    let key = odf::metadata::PrivateKey::from_bytes_padded(b"I-have-a-different-verification-key");

    let predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![
            AccountConfig::test_config_from_name(alice_account_name.clone())
                .set_id(Some(alice_account_id.clone()))
                .set_private_key(key),
            AccountConfig::test_config_from_name(bob_account_name.clone()),
        ],
    };
    let catalog = harness.build_catalog(predefined_accounts_config);

    assert_matches!(harness.run_initialization(&catalog).await, Ok(_));

    // Alice account creation was skipped because of invalid config
    assert_matches!(
        harness
            .try_get_account_by_name(&catalog, &alice_account_name)
            .await,
        None
    );
    assert_matches!(
        harness
            .try_get_account_by_name(&catalog, &bob_account_name)
            .await,
        Some(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_add_key_to_existed_account() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");

    let mut outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(alice_account_name.clone())
        .expected_display_name("alice".to_string())
        .expected_email(Email::parse("alice@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    // 1. Create an account: w/ id but w/o key.
    let alice_private_key = odf::metadata::PrivateKey::from_bytes_padded(b"alice-key");
    let alice_account_id = odf::AccountID::from_signing_key(&alice_private_key);

    let predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![
            AccountConfig::test_config_from_name(alice_account_name.clone())
                .set_id(Some(alice_account_id.clone())),
        ],
    };
    let first_run_alice_account = {
        let first_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&first_run_catalog).await, Ok(_));

        // Let's make sure there is no key
        assert_eq!(
            None,
            harness
                .try_account_secret(&first_run_catalog, &alice_account_id)
                .await
        );

        harness
            .try_get_account_by_name(&first_run_catalog, &alice_account_name)
            .await
            .unwrap()
    };

    // 2. Re-use the account and add a new key
    {
        let mut updated_config = predefined_accounts_config;
        if let Some(ac) = updated_config.predefined.first_mut() {
            *ac = ac.clone().set_private_key(alice_private_key);
        } else {
            unreachable!()
        }
        let second_run_catalog = harness.build_catalog(updated_config);

        assert_matches!(harness.run_initialization(&second_run_catalog).await, Ok(_));

        let second_run_alice_account = harness
            .try_get_account_by_name(&second_run_catalog, &alice_account_name)
            .await
            .unwrap();

        assert_eq!(first_run_alice_account, second_run_alice_account);

        // The key has been added
        assert_matches!(
            harness
                .try_account_secret(&second_run_catalog, &alice_account_id)
                .await,
            Some(_)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_users_change_own_config_data() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");
    let bob_account_name = odf::AccountName::new_unchecked("bob");
    let carol_account_name = odf::AccountName::new_unchecked("carol");

    let mut outbox = MockOutbox::new();
    // 1 run
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(alice_account_name.clone())
        .expected_display_name("alice".to_string())
        .expected_email(Email::parse("alice@example.com").unwrap())
        .expected_times(1)
        .call();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(bob_account_name.clone())
        .expected_display_name("bob".to_string())
        .expected_email(Email::parse("bob@example.com").unwrap())
        .expected_times(1)
        .call();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(carol_account_name.clone())
        .expected_display_name("carol".to_string())
        .expected_email(Email::parse("carol@example.com").unwrap())
        .expected_times(1)
        .call();
    // 2, 3, 4 runs
    let alice_new_email = Email::parse("alice@wonderland.dev").unwrap();
    kamu_accounts::testing::expect_outbox_account_updated()
        .mock_outbox(&mut outbox)
        // w/o id
        .expected_old_email(Email::parse("alice@example.com").unwrap())
        .expected_new_email(alice_new_email.clone())
        .expected_old_account_name(alice_account_name.clone())
        .expected_new_account_name(alice_account_name.clone())
        .expected_old_display_name("alice".to_string())
        .expected_new_display_name("alice".to_string())
        .expected_times(1)
        .call();
    let bob_new_display_name = "Robert".to_string();
    kamu_accounts::testing::expect_outbox_account_updated()
        .mock_outbox(&mut outbox)
        // w/o id
        .expected_old_email(Email::parse("bob@example.com").unwrap())
        .expected_new_email(Email::parse("bob@example.com").unwrap())
        .expected_old_account_name(bob_account_name.clone())
        .expected_new_account_name(bob_account_name.clone())
        .expected_old_display_name("bob".to_string())
        .expected_new_display_name(bob_new_display_name.clone())
        .expected_times(1)
        .call();
    let carol_new_account_name = odf::AccountName::new_unchecked("caroline");
    kamu_accounts::testing::expect_outbox_account_updated()
        .mock_outbox(&mut outbox)
        // w/o id
        .expected_old_email(Email::parse("carol@example.com").unwrap())
        .expected_new_email(Email::parse("carol@example.com").unwrap())
        .expected_old_account_name(carol_account_name.clone())
        .expected_new_account_name(carol_new_account_name.clone())
        .expected_old_display_name("carol".to_string())
        .expected_new_display_name("carol".to_string())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    // 1. Create accounts
    let mut predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![
            AccountConfig::test_config_from_name(alice_account_name.clone()),
            AccountConfig::test_config_from_name(bob_account_name.clone()),
            AccountConfig::test_config_from_name(carol_account_name.clone()),
        ],
    };
    let (first_run_alice_account, first_run_bob_account, first_run_carol_account) = {
        let first_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&first_run_catalog).await, Ok(_));

        let first_run_alice_account = harness
            .try_get_account_by_name(&first_run_catalog, &alice_account_name)
            .await
            .unwrap();
        let first_run_bob_account = harness
            .try_get_account_by_name(&first_run_catalog, &bob_account_name)
            .await
            .unwrap();
        let first_run_carol_account = harness
            .try_get_account_by_name(&first_run_catalog, &carol_account_name)
            .await
            .unwrap();

        (
            first_run_alice_account,
            first_run_bob_account,
            first_run_carol_account,
        )
    };

    // 2. Alice updates her email
    let (second_run_alice_account, second_run_bob_account, second_run_carol_account) = {
        if let Some(ac) = predefined_accounts_config.predefined.get_mut(0) {
            *ac = ac
                .clone()
                .set_email(Email::parse("alice@wonderland.dev").unwrap());
        } else {
            unreachable!()
        }
        let second_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&second_run_catalog).await, Ok(_));

        let second_run_alice_account = harness
            .try_get_account_by_name(&second_run_catalog, &alice_account_name)
            .await
            .unwrap();
        let second_run_bob_account = harness
            .try_get_account_by_name(&second_run_catalog, &bob_account_name)
            .await
            .unwrap();
        let second_run_carol_account = harness
            .try_get_account_by_name(&second_run_catalog, &carol_account_name)
            .await
            .unwrap();

        {
            let Account {
                id: old_id,
                account_name: old_name,
                email: _unused_old_email,
                display_name: old_display_name,
                account_type: old_account_type,
                avatar_url: old_avatar_url,
                registered_at: old_registered_at,
                provider: old_provider,
                provider_identity_key: old_provider_identity_key,
            } = &first_run_alice_account;
            let Account {
                id: new_id,
                account_name: new_name,
                email: _unused_new_email,
                display_name: new_display_name,
                account_type: new_account_type,
                avatar_url: new_avatar_url,
                registered_at: new_registered_at,
                provider: new_provider,
                provider_identity_key: new_provider_identity_key,
            } = &second_run_alice_account;

            assert_eq!(
                (
                    old_id,
                    old_name,
                    // old_email,
                    old_display_name,
                    old_account_type,
                    old_avatar_url,
                    old_registered_at,
                    old_provider,
                    old_provider_identity_key
                ),
                (
                    new_id,
                    new_name,
                    // new_email,
                    new_display_name,
                    new_account_type,
                    new_avatar_url,
                    new_registered_at,
                    new_provider,
                    new_provider_identity_key
                )
            );
            assert_eq!(alice_new_email, second_run_alice_account.email);
        }
        assert_eq!(first_run_bob_account, second_run_bob_account);
        assert_eq!(first_run_carol_account, second_run_carol_account);

        (
            second_run_alice_account,
            second_run_bob_account,
            second_run_carol_account,
        )
    };

    // 3. Bob updates his display name
    let (third_run_alice_account, third_run_bob_account, third_run_carol_account) = {
        if let Some(ac) = predefined_accounts_config.predefined.get_mut(1) {
            *ac = ac.clone().set_display_name(bob_new_display_name.clone());
        } else {
            unreachable!()
        }
        let third_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&third_run_catalog).await, Ok(_));

        let third_run_alice_account = harness
            .try_get_account_by_name(&third_run_catalog, &alice_account_name)
            .await
            .unwrap();
        let third_run_bob_account = harness
            .try_get_account_by_name(&third_run_catalog, &bob_account_name)
            .await
            .unwrap();
        let third_run_carol_account = harness
            .try_get_account_by_name(&third_run_catalog, &carol_account_name)
            .await
            .unwrap();

        assert_eq!(second_run_alice_account, third_run_alice_account);
        {
            let Account {
                id: old_id,
                account_name: old_name,
                email: old_email,
                display_name: _unused_old_display_name,
                account_type: old_account_type,
                avatar_url: old_avatar_url,
                registered_at: old_registered_at,
                provider: old_provider,
                provider_identity_key: old_provider_identity_key,
            } = &second_run_bob_account;
            let Account {
                id: new_id,
                account_name: new_name,
                email: new_email,
                display_name: _unused_new_display_name,
                account_type: new_account_type,
                avatar_url: new_avatar_url,
                registered_at: new_registered_at,
                provider: new_provider,
                provider_identity_key: new_provider_identity_key,
            } = &third_run_bob_account;

            assert_eq!(
                (
                    old_id,
                    old_name,
                    old_email,
                    // old_display_name,
                    old_account_type,
                    old_avatar_url,
                    old_registered_at,
                    old_provider,
                    old_provider_identity_key
                ),
                (
                    new_id,
                    new_name,
                    new_email,
                    // new_display_name,
                    new_account_type,
                    new_avatar_url,
                    new_registered_at,
                    new_provider,
                    new_provider_identity_key
                )
            );
            assert_eq!(bob_new_display_name, third_run_bob_account.display_name);
        }
        assert_eq!(second_run_carol_account, third_run_carol_account);

        (
            third_run_alice_account,
            third_run_bob_account,
            third_run_carol_account,
        )
    };

    // 4. Carol updates her account name
    {
        if let Some(ac) = predefined_accounts_config.predefined.get_mut(2) {
            *ac = ac
                .clone()
                .set_account_name(carol_new_account_name.clone())
                // Display name is same
                .set_display_name("carol".to_string());
        } else {
            unreachable!()
        }
        let fourth_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&fourth_run_catalog).await, Ok(_));

        let fourth_run_alice_account = harness
            .try_get_account_by_name(&fourth_run_catalog, &alice_account_name)
            .await
            .unwrap();
        let fourth_run_bob_account = harness
            .try_get_account_by_name(&fourth_run_catalog, &bob_account_name)
            .await
            .unwrap();
        assert_matches!(
            harness
                .try_get_account_by_name(&fourth_run_catalog, &carol_account_name)
                .await,
            None
        );
        let fourth_run_carol_account = harness
            .try_get_account_by_name(&fourth_run_catalog, &carol_new_account_name)
            .await
            .unwrap();

        assert_eq!(third_run_alice_account, fourth_run_alice_account);
        assert_eq!(third_run_bob_account, fourth_run_bob_account);
        {
            let Account {
                id: old_id,
                account_name: _unused_old_name,
                email: old_email,
                display_name: old_display_name,
                account_type: old_account_type,
                avatar_url: old_avatar_url,
                registered_at: old_registered_at,
                provider: old_provider,
                provider_identity_key: _unused_old_provider_identity_key,
            } = &third_run_carol_account;
            let Account {
                id: new_id,
                account_name: _unused_new_name,
                email: new_email,
                display_name: new_display_name,
                account_type: new_account_type,
                avatar_url: new_avatar_url,
                registered_at: new_registered_at,
                provider: new_provider,
                provider_identity_key: _unused_new_provider_identity_key,
            } = &fourth_run_carol_account;

            assert_eq!(
                (
                    old_id,
                    // old_name,
                    old_email,
                    old_display_name,
                    old_account_type,
                    old_avatar_url,
                    old_registered_at,
                    old_provider,
                    // old_provider_identity_key
                ),
                (
                    new_id,
                    // new_name,
                    new_email,
                    new_display_name,
                    new_account_type,
                    new_avatar_url,
                    new_registered_at,
                    new_provider,
                    // new_provider_identity_key
                )
            );
            assert_eq!(
                carol_new_account_name,
                fourth_run_carol_account.account_name
            );
            assert_eq!(
                "caroline".to_string(),
                fourth_run_carol_account.provider_identity_key
            );
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_alice_change_rebac_properties() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");

    let mut outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(alice_account_name.clone())
        .expected_display_name("alice".to_string())
        .expected_email(Email::parse("alice@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    // 1. Create an account
    let predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![AccountConfig::test_config_from_name(
            alice_account_name.clone(),
        )],
    };
    let first_run_alice_account = {
        let first_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&first_run_catalog).await, Ok(_));

        let alice_account = harness
            .try_get_account_by_name(&first_run_catalog, &alice_account_name)
            .await
            .unwrap();

        assert_eq!(
            kamu_auth_rebac_services::DefaultAccountProperties::default(),
            harness
                .try_get_account_properties(&first_run_catalog, &alice_account.id)
                .await
        );

        alice_account
    };

    // 2. Re-use the account but update its ReBAC properties
    {
        let mut updated_config = predefined_accounts_config;
        if let Some(ac) = updated_config.predefined.first_mut() {
            *ac = ac.clone().set_properties(vec![
                AccountPropertyName::IsAdmin,
                AccountPropertyName::CanProvisionAccounts,
            ]);
        } else {
            unreachable!()
        }
        let second_run_catalog = harness.build_catalog(updated_config);

        assert_matches!(harness.run_initialization(&second_run_catalog).await, Ok(_));

        let second_run_alice_account = harness
            .try_get_account_by_name(&second_run_catalog, &alice_account_name)
            .await
            .unwrap();

        assert_eq!(first_run_alice_account, second_run_alice_account);

        assert_eq!(
            kamu_auth_rebac::AccountProperties {
                is_admin: true,
                can_provision_accounts: true
            },
            harness
                .try_get_account_properties(&second_run_catalog, &second_run_alice_account.id)
                .await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mt_skip_accounts_with_conflicting_fields() {
    let alice_account_name = odf::AccountName::new_unchecked("alice");
    let bob_account_name = odf::AccountName::new_unchecked("bob");
    let carol_account_name = odf::AccountName::new_unchecked("carol");

    let mut outbox = MockOutbox::new();
    // Only Alice's account will be created -- rest skipped
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut outbox)
        .expected_account_name(alice_account_name.clone())
        .expected_display_name("alice".to_string())
        .expected_email(Email::parse("alice@example.com").unwrap())
        .expected_times(1)
        .call();

    let harness = PredefinedAccountsRegistratorHarness::builder()
        .mock_outbox(outbox)
        .build();

    let predefined_accounts_config = PredefinedAccountsConfig {
        predefined: vec![AccountConfig::test_config_from_name(
            alice_account_name.clone(),
        )],
    };

    // 1. Create Alice's account
    let first_run_alice_account = {
        let first_run_catalog = harness.build_catalog(predefined_accounts_config.clone());

        assert_matches!(harness.run_initialization(&first_run_catalog).await, Ok(_));

        assert_matches!(
            harness
                .try_get_account_by_name(&first_run_catalog, &bob_account_name)
                .await,
            None
        );
        assert_matches!(
            harness
                .try_get_account_by_name(&first_run_catalog, &carol_account_name)
                .await,
            None
        );

        harness
            .try_get_account_by_name(&first_run_catalog, &alice_account_name)
            .await
            .unwrap()
    };

    // 2. Create Alice's account
    {
        let second_run_catalog = harness.build_catalog({
            let mut c = predefined_accounts_config;
            // Bob is trying to steal the account_name / provider_identity_key
            c.predefined.push(
                AccountConfig::test_config_from_name(bob_account_name.clone())
                    .set_account_name(alice_account_name.clone()),
            );
            // Carol is trying to steal the email
            c.predefined.push(
                AccountConfig::test_config_from_name(carol_account_name.clone())
                    .set_email(Email::parse("alice@example.com").unwrap()),
            );
            c
        });

        assert_matches!(harness.run_initialization(&second_run_catalog).await, Ok(_));

        let second_run_alice_account = harness
            .try_get_account_by_name(&second_run_catalog, &alice_account_name)
            .await
            .unwrap();

        assert_eq!(first_run_alice_account, second_run_alice_account);

        assert_matches!(
            harness
                .try_get_account_by_name(&second_run_catalog, &bob_account_name)
                .await,
            None
        );
        assert_matches!(
            harness
                .try_get_account_by_name(&second_run_catalog, &carol_account_name)
                .await,
            None
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PredefinedAccountsRegistratorHarness {
    catalog_without_predefined_accounts_config: dill::Catalog,
}

#[bon::bon]
impl PredefinedAccountsRegistratorHarness {
    #[builder]
    pub fn new(mock_outbox: MockOutbox) -> Self {
        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            // PredefinedAccountsRegistrator
            b.add::<kamu_accounts_services::PredefinedAccountsRegistrator>();
            b.add::<kamu_accounts_services::AccountServiceImpl>();
            b.add::<kamu_auth_rebac_services::RebacServiceImpl>();
            b.add_value(kamu_auth_rebac_services::DefaultAccountProperties::default());
            b.add::<kamu_accounts_services::UpdateAccountUseCaseImpl>();
            b.add::<kamu_accounts_services::CreateAccountUseCaseImpl>();
            b.add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>();
            b.add_value(kamu_accounts::DidSecretEncryptionConfig::sample());

            // AccountServiceImpl
            b.add::<kamu_accounts_inmem::InMemoryAccountRepository>();

            // RebacServiceImpl
            b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();
            b.add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default());

            // UpdateAccountUseCaseImpl
            b.add_value(mock_outbox)
                .bind::<dyn messaging_outbox::Outbox, MockOutbox>();
            b.add::<time_source::SystemTimeSourceDefault>();
            b.add_value(kamu_accounts_services::utils::MockAccountAuthorizationHelper::new())
                .bind::<dyn kamu_accounts_services::utils::AccountAuthorizationHelper, kamu_accounts_services::utils::MockAccountAuthorizationHelper>();

            b.build()
        };

        Self {
            catalog_without_predefined_accounts_config: catalog,
        }
    }

    pub fn build_catalog(
        &self,
        predefined_accounts_config: PredefinedAccountsConfig,
    ) -> dill::Catalog {
        dill::CatalogBuilder::new_chained(&self.catalog_without_predefined_accounts_config)
            .add_value(predefined_accounts_config)
            .build()
    }

    pub async fn run_initialization(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        use init_on_startup::InitOnStartup;

        let predefined_accounts_registrator = catalog
            .get_one::<kamu_accounts_services::PredefinedAccountsRegistrator>()
            .unwrap();

        predefined_accounts_registrator.run_initialization().await
    }

    pub async fn try_get_account_by_name(
        &self,
        catalog: &dill::Catalog,
        account_name: &odf::AccountName,
    ) -> Option<Account> {
        let account_service = catalog
            .get_one::<dyn kamu_accounts::AccountService>()
            .unwrap();

        account_service.account_by_name(account_name).await.unwrap()
    }

    pub async fn try_account_secret(
        &self,
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
    ) -> Option<kamu_accounts::DidSecretKey> {
        let did_secret_key_repo = catalog
            .get_one::<dyn kamu_accounts::DidSecretKeyRepository>()
            .unwrap();

        use odf::metadata::AsStackString;

        let account_id_stack = account_id.as_stack_string();
        let account_entity = kamu_accounts::DidEntity::new_account(account_id_stack.as_str());

        use kamu_accounts::GetDidSecretKeyError as E;

        match did_secret_key_repo
            .get_did_secret_key(&account_entity)
            .await
        {
            Ok(did_secret_key) => Some(did_secret_key),
            Err(E::NotFound(_)) => None,
            Err(e) => panic!("Unexpected: {e:?}"),
        }
    }

    pub async fn try_get_account_properties(
        &self,
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
    ) -> kamu_auth_rebac::AccountProperties {
        let rebac_service = catalog
            .get_one::<dyn kamu_auth_rebac::RebacService>()
            .unwrap();

        rebac_service
            .get_account_properties(account_id)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
