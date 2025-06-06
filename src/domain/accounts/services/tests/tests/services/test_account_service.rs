// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use database_common::NoOpDatabasePlugin;
use email_utils::Email;
use kamu_accounts::{
    AccountConfig,
    AccountService,
    AccountServiceExt,
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKey,
    DidSecretKeyRepository,
    PredefinedAccountsConfig,
    SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::AccountName;
use odf::metadata::{DidKey, DidOdf};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WASYA: &str = "wasya";
const PETYA: &str = "petya";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_can_find_account_by_all_means() {
    let catalog = make_catalog().await;
    let account_svc = catalog.get_one::<dyn AccountService>().unwrap();

    let wasya_account = {
        let maybe_wasya_account = account_svc
            .account_by_name(&AccountName::new_unchecked(WASYA))
            .await
            .unwrap();
        assert!(maybe_wasya_account.is_some());
        maybe_wasya_account.unwrap()
    };

    let maybe_wasya_by_id = account_svc.account_by_id(&wasya_account.id).await.unwrap();
    assert_matches!(maybe_wasya_by_id, Some(wasya_by_id) if wasya_by_id == wasya_account);

    let maybe_id = account_svc
        .find_account_id_by_name(&wasya_account.account_name)
        .await
        .unwrap();
    assert_matches!(maybe_id, Some(id) if id == wasya_account.id);

    let maybe_name = account_svc
        .find_account_name_by_id(&wasya_account.id)
        .await
        .unwrap();
    assert_matches!(maybe_name, Some(name) if name == wasya_account.account_name);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_find() {
    let catalog = make_catalog().await;
    let account_svc = catalog.get_one::<dyn AccountService>().unwrap();

    let wasya_id = account_svc
        .find_account_id_by_name(&AccountName::new_unchecked(WASYA))
        .await
        .unwrap()
        .unwrap();
    let petya_id = account_svc
        .find_account_id_by_name(&AccountName::new_unchecked(PETYA))
        .await
        .unwrap()
        .unwrap();

    let mut accounts = account_svc
        .get_accounts_by_ids(&[wasya_id.clone(), petya_id.clone()])
        .await
        .unwrap();
    pretty_assertions::assert_eq!(2, accounts.len());
    accounts.sort_by(|acc1, acc2| acc1.account_name.cmp(&acc2.account_name));
    pretty_assertions::assert_eq!(PETYA, accounts[0].account_name.as_str());
    pretty_assertions::assert_eq!(WASYA, accounts[1].account_name.as_str());

    let accounts_map = account_svc
        .get_account_map(&[wasya_id.clone(), petya_id.clone()])
        .await
        .unwrap();
    pretty_assertions::assert_eq!(2, accounts.len());
    assert!(
        accounts_map
            .get(&wasya_id)
            .is_some_and(|a| a.account_name.as_str() == WASYA)
    );
    assert!(
        accounts_map
            .get(&petya_id)
            .is_some_and(|a| a.account_name.as_str() == PETYA)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account() {
    let catalog = make_catalog().await;
    let account_svc = catalog.get_one::<dyn AccountService>().unwrap();
    let did_secret_key_repo = catalog.get_one::<dyn DidSecretKeyRepository>().unwrap();

    let new_account_name = AccountName::new_unchecked("new_account");
    account_svc
        .create_password_account(&new_account_name, Email::parse("new_email@com").unwrap())
        .await
        .unwrap();

    let created_account_id = account_svc
        .find_account_id_by_name(&new_account_name)
        .await
        .unwrap()
        .unwrap();

    let created_account_did_secret_key = did_secret_key_repo
        .get_did_secret_key(&DidEntity::new_account(created_account_id.to_string()))
        .await
        .unwrap();

    let did_private_key = created_account_did_secret_key
        .get_decrypted_private_key(&DidSecretEncryptionConfig::sample().encryption_key.unwrap())
        .unwrap();

    let public_key = did_private_key.verifying_key().to_bytes();
    let did_odf =
        DidOdf::from(DidKey::new(odf::metadata::Multicodec::Ed25519Pub, &public_key).unwrap());

    // Compare original account_id from db and id generated from a stored private
    // key
    pretty_assertions::assert_eq!(created_account_id.as_did_odf(), Some(&did_odf));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_did_secret_key_generation() {
    let account_did = odf::AccountID::new_generated_ed25519();
    let new_did_secret_key =
        DidSecretKey::try_new(&account_did.0.into(), SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY).unwrap();

    let original_value = new_did_secret_key
        .get_decrypted_private_key(SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY)
        .unwrap();

    let public_key = original_value.verifying_key().to_bytes();
    let did_odf =
        DidOdf::from(DidKey::new(odf::metadata::Multicodec::Ed25519Pub, &public_key).unwrap());

    pretty_assertions::assert_eq!(account_did.1.as_did_odf(), Some(&did_odf));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_catalog() -> dill::Catalog {
    let mut b = dill::CatalogBuilder::new();

    let mut predefined_account_config = PredefinedAccountsConfig::new();
    for account_name in [WASYA, PETYA] {
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
        .add::<PredefinedAccountsRegistrator>()
        .add::<DummyOutboxImpl>();

    NoOpDatabasePlugin::init_database_components(&mut b);

    let catalog = b.build();

    init_on_startup::run_startup_jobs(&catalog).await.unwrap();

    catalog
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
