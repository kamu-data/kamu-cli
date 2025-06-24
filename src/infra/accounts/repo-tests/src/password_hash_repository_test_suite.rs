// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use kamu_accounts::{
    AccountProvider,
    AccountRepository,
    ModifyPasswordHashError,
    PasswordHashRepository,
};

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

    let account_wasya = make_test_account(
        "wasya",
        "wasya@example.com",
        AccountProvider::Password.into(),
        "wasya",
    );
    let account_petya = make_test_account(
        "petya",
        "petya@example.com",
        AccountProvider::Password.into(),
        "petya",
    );

    account_repo.save_account(&account_wasya).await.unwrap();
    account_repo.save_account(&account_petya).await.unwrap();

    const PASSWORD_WASYA: &str = "password_wasya";
    const PASSWORD_PETYA: &str = "password_petya";

    let salt_wasya = generate_salt();
    let salt_petya = generate_salt();

    let hash_wasya = make_password_hash(PASSWORD_WASYA, &salt_wasya);
    let hash_petya = make_password_hash(PASSWORD_PETYA, &salt_petya);

    password_hash_repo
        .save_password_hash(&account_wasya.id, hash_wasya.to_string())
        .await
        .unwrap();

    password_hash_repo
        .save_password_hash(&account_petya.id, hash_petya.to_string())
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

pub async fn test_modify_password(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let password_hash_repo = catalog.get_one::<dyn PasswordHashRepository>().unwrap();

    let account_petya = make_test_account(
        "petya",
        "petya@example.com",
        AccountProvider::Password.into(),
        "petya",
    );

    account_repo.save_account(&account_petya).await.unwrap();

    let password_petya = "password_petya";
    let salt = generate_salt();
    let hash_petya = make_password_hash(password_petya, &salt);

    password_hash_repo
        .save_password_hash(&account_petya.id, hash_petya.to_string())
        .await
        .unwrap();

    let result = password_hash_repo
        .find_password_hash_by_account_name(&account_petya.account_name)
        .await
        .unwrap();
    assert!(result.is_some_and(|hash| {
        assert_eq!(hash, hash_petya.to_string());
        true
    }));

    let password_petya = "new_password_petya";
    let salt = generate_salt();
    let hash_petya = make_password_hash(password_petya, &salt);

    password_hash_repo
        .modify_password_hash(&account_petya.id, hash_petya.to_string())
        .await
        .unwrap();

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

pub async fn test_modify_password_non_existing(catalog: &Catalog) {
    let password_hash_repo = catalog.get_one::<dyn PasswordHashRepository>().unwrap();

    assert_matches!(
        password_hash_repo
            .modify_password_hash(
                &odf::AccountID::new_seeded_ed25519(b"foo"),
                "password_hash".to_string(),
            )
            .await,
        Err(ModifyPasswordHashError::AccountNotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
