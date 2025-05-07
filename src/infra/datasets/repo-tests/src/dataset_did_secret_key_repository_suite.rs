// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound, Utc};
use crypto_utils::{DidSecretKey, SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY};
use kamu_accounts::AccountRepository;
use kamu_datasets::{DatasetDidSecretKeyRepository, DatasetEntry, DatasetEntryRepository};

use crate::helpers::new_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_dataset_did_secret_keys(catalog: &dill::Catalog) {
    let dataset_did_secret_key_repo = catalog
        .get_one::<dyn DatasetDidSecretKeyRepository>()
        .unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let (dataset_did_signing_key, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let creator_id = account.id.clone();
    let owner_name = account.account_name.clone();
    let dataset_alias = odf::DatasetName::new_unchecked("foo");
    let created_at = Utc::now().round_subsecs(6);

    let entry_foo = DatasetEntry::new(
        dataset_id.clone(),
        creator_id.clone(),
        owner_name,
        dataset_alias,
        created_at,
        odf::DatasetKind::Root,
    );
    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let did_secret_key = DidSecretKey::try_new(
        &dataset_did_signing_key.into(),
        SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
    )
    .unwrap();

    dataset_did_secret_key_repo
        .save_did_secret_key(&dataset_id, &creator_id, &did_secret_key)
        .await
        .unwrap();

    let dataset_did_secret_keys = dataset_did_secret_key_repo
        .get_did_secret_keys_by_creator_id(&creator_id)
        .await
        .unwrap();

    assert_eq!(dataset_did_secret_keys, vec![did_secret_key]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
