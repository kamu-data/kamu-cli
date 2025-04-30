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
use kamu_accounts::{AccountConfig, AccountService, AccountServiceExt, PredefinedAccountsConfig};
use kamu_accounts_inmem::InMemoryAccountRepository;
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
use odf::AccountName;

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
    assert_eq!(2, accounts.len());
    accounts.sort_by(|acc1, acc2| acc1.account_name.cmp(&acc2.account_name));
    assert_eq!(accounts[0].account_name.as_str(), PETYA);
    assert_eq!(accounts[1].account_name.as_str(), WASYA);

    let accounts_map = account_svc
        .get_account_map(&[wasya_id.clone(), petya_id.clone()])
        .await
        .unwrap();
    assert_eq!(2, accounts.len());
    assert!(accounts_map
        .get(&wasya_id)
        .is_some_and(|a| a.account_name.as_str() == WASYA));
    assert!(accounts_map
        .get(&petya_id)
        .is_some_and(|a| a.account_name.as_str() == PETYA));
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
        .add::<LoginPasswordAuthProvider>()
        .add::<RebacServiceImpl>()
        .add::<InMemoryRebacRepository>()
        .add_value(DefaultAccountProperties::default())
        .add_value(DefaultDatasetProperties::default())
        .add::<PredefinedAccountsRegistrator>();

    NoOpDatabasePlugin::init_database_components(&mut b);

    let catalog = b.build();

    init_on_startup::run_startup_jobs(&catalog).await.unwrap();

    catalog
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
