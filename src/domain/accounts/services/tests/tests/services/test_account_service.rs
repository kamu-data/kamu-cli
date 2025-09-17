// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    AccountConfig,
    AccountService,
    AccountServiceExt,
    DidSecretEncryptionConfig,
    DidSecretKey,
    PredefinedAccountsConfig,
    SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::metadata::{DidKey, DidOdf};
use pretty_assertions::{assert_eq, assert_matches};
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
            .account_by_name(&odf::AccountName::new_unchecked(WASYA))
            .await
            .unwrap();
        assert!(maybe_wasya_account.is_some());
        maybe_wasya_account.unwrap()
    };

    let maybe_wasya_by_id = account_svc
        .try_get_account_by_id(&wasya_account.id)
        .await
        .unwrap();
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
        .find_account_id_by_name(&odf::AccountName::new_unchecked(WASYA))
        .await
        .unwrap()
        .unwrap();
    let petya_id = account_svc
        .find_account_id_by_name(&odf::AccountName::new_unchecked(PETYA))
        .await
        .unwrap()
        .unwrap();
    let not_found_account_id = odf::AccountID::new_generated_ed25519().1;
    let not_found_account_name = "I.cannot.be.found";

    {
        let lookup = account_svc
            .get_accounts_by_ids(&[&wasya_id, &petya_id, &not_found_account_id])
            .await
            .unwrap();
        let found_accounts = lookup
            .found
            .iter()
            .map(|a| (a.account_name.as_str(), &a.id))
            .collect::<Vec<_>>();
        assert_eq!([(PETYA, &petya_id), (WASYA, &wasya_id)], *found_accounts);

        let not_found_account_ids = lookup
            .not_found
            .iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        assert_eq!([&not_found_account_id], *not_found_account_ids);
    }
    {
        fn odf_name(s: &str) -> odf::AccountName {
            odf::AccountName::new_unchecked(s)
        }

        let lookup = account_svc
            .get_accounts_by_names(&[
                &odf_name(WASYA),
                &odf_name(PETYA),
                &odf_name(not_found_account_name),
            ])
            .await
            .unwrap();
        let found_accounts = lookup
            .found
            .iter()
            .map(|a| (a.account_name.as_str(), &a.id))
            .collect::<Vec<_>>();
        assert_eq!([(PETYA, &petya_id), (WASYA, &wasya_id)], *found_accounts);

        let not_found_account_names = lookup
            .not_found
            .iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        assert_eq!(
            [&odf_name(not_found_account_name)],
            *not_found_account_names
        );
    }
    {
        let map = account_svc
            .get_account_map(&[&wasya_id, &petya_id, &not_found_account_id])
            .await
            .unwrap();
        assert_eq!(2, map.len());
        assert!(
            map.get(&wasya_id)
                .is_some_and(|a| a.account_name.as_str() == WASYA)
        );
        assert!(
            map.get(&petya_id)
                .is_some_and(|a| a.account_name.as_str() == PETYA)
        );
        assert!(!map.contains_key(&not_found_account_id));
    }
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

    assert_eq!(account_did.1.as_did_odf(), Some(&did_odf));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_catalog() -> dill::Catalog {
    let mut b = dill::CatalogBuilder::new();

    let mut predefined_account_config = PredefinedAccountsConfig::new();
    for account_name in [WASYA, PETYA] {
        predefined_account_config
            .predefined
            .push(AccountConfig::test_config_from_name(
                odf::AccountName::new_unchecked(account_name),
            ));
    }

    b.add::<AccountServiceImpl>()
        .add::<InMemoryAccountRepository>()
        .add_value(predefined_account_config)
        .add::<SystemTimeSourceDefault>()
        .add::<LoginPasswordAuthProvider>()
        .add::<RebacServiceImpl>()
        .add::<UpdateAccountUseCaseImpl>()
        .add::<CreateAccountUseCaseImpl>()
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
