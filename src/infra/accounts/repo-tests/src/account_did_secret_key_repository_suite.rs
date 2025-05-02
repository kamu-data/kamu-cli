// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_utils::{DidSecretKey, SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY};
use kamu_accounts::{AccountDidSecretKeyRepository, AccountRepository};

use crate::{make_test_account, GITHUB_ACCOUNT_ID_PETYA, GITHUB_ACCOUNT_ID_WASYA};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_did_secret_keys(catalog: &dill::Catalog) {
    let owner_account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let new_account_did = odf::AccountID::new_generated_ed25519();
    let new_account = make_test_account(
        "petya",
        "petya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_PETYA,
        Some(new_account_did.1.clone()),
    );

    let did_secret_key = DidSecretKey::try_new(
        &new_account_did.0.into(),
        SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
    )
    .unwrap();

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let account_did_secret_key_repository = catalog
        .get_one::<dyn AccountDidSecretKeyRepository>()
        .unwrap();

    account_repo.save_account(&owner_account).await.unwrap();
    account_repo.save_account(&new_account).await.unwrap();

    let account_did_secret_keys = account_did_secret_key_repository
        .get_did_secret_keys_by_owner_id(&owner_account.id)
        .await
        .unwrap();

    assert_eq!(account_did_secret_keys, vec![]);

    account_did_secret_key_repository
        .save_did_secret_key(&new_account.id, &owner_account.id, &did_secret_key)
        .await
        .unwrap();

    let account_did_secret_keys = account_did_secret_key_repository
        .get_did_secret_keys_by_owner_id(&owner_account.id)
        .await
        .unwrap();

    assert_eq!(account_did_secret_keys, vec![did_secret_key]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
