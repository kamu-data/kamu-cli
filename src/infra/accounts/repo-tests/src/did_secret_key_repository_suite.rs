// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound, Utc};
use email_utils::Email;
use kamu_accounts::{
    Account,
    AccountRepository,
    AccountType,
    DidEntity,
    DidEntityType,
    DidSecretKey,
    DidSecretKeyRepository,
    SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_did_secret_keys(catalog: &dill::Catalog) {
    let creator_account = Account {
        id: odf::AccountID::new_seeded_ed25519("foo".as_bytes()),
        account_name: odf::AccountName::new_unchecked("foo"),
        email: Email::parse("foo@example.com").unwrap(),
        display_name: String::from("foo"),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Utc::now().round_subsecs(6),
        provider: String::from("foo"),
        provider_identity_key: String::from("foo"),
    };
    let new_account_did = odf::AccountID::new_generated_ed25519();
    let new_dataset_did = odf::AccountID::new_generated_ed25519();

    let account_did_secret_key = DidSecretKey::try_new(
        &new_account_did.0.into(),
        SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
    )
    .unwrap();
    let dataset_did_secret_key = DidSecretKey::try_new(
        &new_dataset_did.0.into(),
        SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
    )
    .unwrap();

    let account_repository = catalog.get_one::<dyn AccountRepository>().unwrap();
    let did_secret_key_repository = catalog.get_one::<dyn DidSecretKeyRepository>().unwrap();

    account_repository
        .save_account(&creator_account)
        .await
        .unwrap();

    let all_did_secret_keys = did_secret_key_repository
        .get_did_secret_keys_by_creator_id(&creator_account.id, None)
        .await
        .unwrap();

    assert_eq!(all_did_secret_keys, vec![]);

    did_secret_key_repository
        .save_did_secret_key(
            &DidEntity::new_account(new_account_did.1.to_string()),
            &creator_account.id,
            &account_did_secret_key,
        )
        .await
        .unwrap();
    did_secret_key_repository
        .save_did_secret_key(
            &DidEntity::new_dataset(new_dataset_did.1.to_string()),
            &creator_account.id,
            &dataset_did_secret_key,
        )
        .await
        .unwrap();

    let account_did_secret_keys = did_secret_key_repository
        .get_did_secret_keys_by_creator_id(&creator_account.id, Some(DidEntityType::Account))
        .await
        .unwrap();
    assert_eq!(
        account_did_secret_keys,
        vec![account_did_secret_key.clone()]
    );

    let dataset_did_secret_keys = did_secret_key_repository
        .get_did_secret_keys_by_creator_id(&creator_account.id, Some(DidEntityType::Dataset))
        .await
        .unwrap();
    assert_eq!(
        dataset_did_secret_keys,
        vec![dataset_did_secret_key.clone()]
    );

    let all_did_secret_keys = did_secret_key_repository
        .get_did_secret_keys_by_creator_id(&creator_account.id, None)
        .await
        .unwrap();
    assert_eq!(
        all_did_secret_keys,
        vec![account_did_secret_key, dataset_did_secret_key]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
