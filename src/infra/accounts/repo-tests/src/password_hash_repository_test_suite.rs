// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_accounts::{AccountRepository, PasswordHashRepository, PROVIDER_PASSWORD};

use crate::{generate_salt, make_password_hash, make_test_account};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_password_stored(catalog: &Catalog) {
    let password_hash_repo = catalog.get_one::<dyn PasswordHashRepository>().unwrap();

    let account_name = odf::AccountName::new_unchecked("I don't exist");
    let result = password_hash_repo
        .find_password_hash_by_account_name(&account_name)
        .await
        .unwrap();
    assert!(result.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store_couple_account_passwords(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let password_hash_repo = catalog.get_one::<dyn PasswordHashRepository>().unwrap();

    let account_wasya = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");
    let account_petya = make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya");

    account_repo.create_account(&account_wasya).await.unwrap();
    account_repo.create_account(&account_petya).await.unwrap();

    const PASSWORD_WASYA: &str = "password_wasya";
    const PASSWORD_PETYA: &str = "password_petya";

    let salt_wasya = generate_salt();
    let salt_petya = generate_salt();

    let hash_wasya = make_password_hash(PASSWORD_WASYA, &salt_wasya);
    let hash_petya = make_password_hash(PASSWORD_PETYA, &salt_petya);

    password_hash_repo
        .save_password_hash(&account_wasya.account_name, hash_wasya.to_string())
        .await
        .unwrap();

    password_hash_repo
        .save_password_hash(&account_petya.account_name, hash_petya.to_string())
        .await
        .unwrap();

    let result = password_hash_repo
        .find_password_hash_by_account_name(&account_wasya.account_name)
        .await
        .unwrap();
    assert!(result.is_some_and(|hash| {
        assert_eq!(hash, hash_wasya.to_string());
        true
    }));

    let result = password_hash_repo
        .find_password_hash_by_account_name(&account_petya.account_name)
        .await
        .unwrap();
    assert!(result.is_some_and(|hash| {
        assert_eq!(hash, hash_petya.to_string());
        true
    }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
