// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_accounts::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_did_secret_keys(catalog: &dill::Catalog) {
    let did_secret_key_repository = catalog.get_one::<dyn DidSecretKeyRepository>().unwrap();

    {
        use odf::metadata::AsStackString;

        let (account_key, account_id) = odf::AccountID::new_generated_ed25519();
        let account_did_secret_key =
            DidSecretKey::try_new(&account_key.into(), SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY)
                .unwrap();
        let account_id = account_id.as_stack_string();

        test(
            did_secret_key_repository.as_ref(),
            account_did_secret_key,
            DidEntity::new_account(account_id.as_str()),
        )
        .await;
    }
    {
        let (dataset_key, dataset_id) = odf::DatasetID::new_generated_ed25519();
        let dataset_did_secret_key =
            DidSecretKey::try_new(&dataset_key.into(), SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY)
                .unwrap();
        let dataset_id = dataset_id.as_did_str().to_stack_string();

        test(
            did_secret_key_repository.as_ref(),
            dataset_did_secret_key,
            DidEntity::new_dataset(dataset_id.as_str()),
        )
        .await;
    }

    async fn test(
        did_secret_key_repository: &dyn DidSecretKeyRepository,
        did_secret_key: DidSecretKey,
        did_entity: DidEntity<'_>,
    ) {
        assert_matches!(
            did_secret_key_repository
                .get_did_secret_key(&did_entity)
                .await,
            Err(GetDidSecretKeyError::NotFound(e))
                if e.entity == did_entity
        );

        assert_matches!(
            did_secret_key_repository
                .save_did_secret_key(&did_entity, &did_secret_key)
                .await,
            Ok(_)
        );

        assert_matches!(
            did_secret_key_repository
                .get_did_secret_key(&did_entity)
                .await,
            Ok(got_did_secret_key)
                if got_did_secret_key == did_secret_key
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
