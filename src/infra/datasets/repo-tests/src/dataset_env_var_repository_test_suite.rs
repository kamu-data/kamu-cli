// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{SubsecRound, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use kamu_accounts::AccountRepository;
use kamu_datasets::{
    DatasetEntryRepository,
    DatasetEnvVar,
    DatasetEnvVarRepository,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
    UpsertDatasetEnvVarStatus,
    SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
};
use opendatafabric::DatasetID;
use secrecy::SecretString;
use uuid::Uuid;

use crate::helpers::{new_account, new_dataset_entry_with};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_missing_dataset_env_var_not_found(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_env_var_result = dataset_env_var_repo
        .get_dataset_env_var_by_id(&Uuid::new_v4())
        .await;
    assert_matches!(
        dataset_env_var_result,
        Err(GetDatasetEnvVarError::NotFound(_))
    );

    let dataset_env_var_result = dataset_env_var_repo
        .get_dataset_env_var_by_key_and_dataset_id("foo", &DatasetID::new_seeded_ed25519(b"foo"))
        .await;
    assert_matches!(
        dataset_env_var_result,
        Err(GetDatasetEnvVarError::NotFound(_))
    );

    let dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &DatasetID::new_seeded_ed25519(b"foo"),
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();
    assert!(dataset_env_vars.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_get_dataset_env_var(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let dataset_env_var_key = "foo";
    let dataset_env_var_key_string = "foo_value".to_string();
    let dataset_env_var_value =
        DatasetEnvVarValue::Secret(SecretString::from(dataset_env_var_key_string.clone()));

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo");
    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let new_dataset_env_var = DatasetEnvVar::new(
        dataset_env_var_key,
        Utc::now().round_subsecs(6),
        &dataset_env_var_value,
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;

    assert!(save_result.is_ok());

    let db_dataset_env_var = dataset_env_var_repo
        .get_dataset_env_var_by_id(&new_dataset_env_var.id)
        .await
        .unwrap();
    assert_eq!(db_dataset_env_var, new_dataset_env_var);

    let db_dataset_env_var = dataset_env_var_repo
        .get_dataset_env_var_by_key_and_dataset_id(dataset_env_var_key, &entry_foo.id)
        .await
        .unwrap();
    assert_eq!(db_dataset_env_var, new_dataset_env_var);

    let db_dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &entry_foo.id,
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();
    assert_eq!(db_dataset_env_vars, vec![new_dataset_env_var]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_get_multiple_dataset_env_vars(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let secret_dataset_env_var_key = "foo";
    let secret_dataset_env_var_key_string = "foo_value".to_string();
    let secret_dataset_env_var_value = DatasetEnvVarValue::Secret(SecretString::from(
        secret_dataset_env_var_key_string.clone(),
    ));

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo");

    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let new_secret_dataset_env_var = DatasetEnvVar::new(
        secret_dataset_env_var_key,
        Utc::now().round_subsecs(6),
        &secret_dataset_env_var_value,
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();

    let dataset_env_var_key = "bar";
    let dataset_env_var_key_string = "bar_value".to_string();
    let dataset_env_var_value = DatasetEnvVarValue::Regular(dataset_env_var_key_string.clone());

    let new_dataset_env_var = DatasetEnvVar::new(
        dataset_env_var_key,
        Utc::now().round_subsecs(6),
        &dataset_env_var_value,
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();

    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;
    assert!(save_result.is_ok());
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_secret_dataset_env_var)
        .await;
    assert!(save_result.is_ok());

    let mut db_dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &entry_foo.id,
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();
    db_dataset_env_vars.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    assert_eq!(
        db_dataset_env_vars,
        vec![new_secret_dataset_env_var, new_dataset_env_var]
    );

    let db_dataset_env_vars_count = dataset_env_var_repo
        .get_all_dataset_env_vars_count_by_dataset_id(&entry_foo.id)
        .await
        .unwrap();

    assert_eq!(db_dataset_env_vars_count, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_env_vars(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo");

    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let new_dataset_env_var = DatasetEnvVar::new(
        "foo",
        Utc::now().round_subsecs(6),
        &DatasetEnvVarValue::Regular("foo".to_string()),
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let new_bar_dataset_env_var = DatasetEnvVar::new(
        "bar",
        Utc::now().round_subsecs(6),
        &DatasetEnvVarValue::Regular("bar".to_string()),
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;
    assert!(save_result.is_ok());
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_bar_dataset_env_var)
        .await;
    assert!(save_result.is_ok());

    let delete_result = dataset_env_var_repo
        .delete_dataset_env_var(&Uuid::new_v4())
        .await;

    assert_matches!(delete_result, Err(DeleteDatasetEnvVarError::NotFound(_)));

    let delete_result = dataset_env_var_repo
        .delete_dataset_env_var(&new_dataset_env_var.id)
        .await;

    assert!(delete_result.is_ok());

    let db_dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &entry_foo.id,
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();

    assert_eq!(db_dataset_env_vars, vec![new_bar_dataset_env_var]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_upsert_dataset_env_vars(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo");

    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let mut new_dataset_env_var = DatasetEnvVar::new(
        "foo",
        Utc::now().round_subsecs(6),
        &DatasetEnvVarValue::Secret(SecretString::from("foo")),
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let upsert_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;
    assert_matches!(upsert_result, Ok(res) if res.status == UpsertDatasetEnvVarStatus::Created);

    let upsert_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;
    assert_matches!(upsert_result, Ok(res) if res.status == UpsertDatasetEnvVarStatus::UpToDate);

    let (new_value, new_nonce) = new_dataset_env_var
        .generate_new_value(
            &DatasetEnvVarValue::Regular("new_foo".to_string()),
            SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
        )
        .unwrap();
    new_dataset_env_var.value.clone_from(&new_value);
    new_dataset_env_var.secret_nonce.clone_from(&new_nonce);

    let upsert_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;

    assert_matches!(upsert_result, Ok(res) if res.status == UpsertDatasetEnvVarStatus::Updated);

    let db_dataset_env_var = dataset_env_var_repo
        .get_dataset_env_var_by_id(&new_dataset_env_var.id)
        .await
        .unwrap();
    assert_eq!(db_dataset_env_var.secret_nonce, new_nonce);
    assert_eq!(
        db_dataset_env_var
            .get_exposed_decrypted_value(SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY)
            .unwrap(),
        std::str::from_utf8(new_value.as_slice()).unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_all_dataset_env_vars(catalog: &Catalog) {
    let dataset_env_var_repo = catalog.get_one::<dyn DatasetEnvVarRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let entry_foo = new_dataset_entry_with(&account, "foo");

    dataset_entry_repo
        .save_dataset_entry(&entry_foo)
        .await
        .unwrap();

    let new_dataset_env_var = DatasetEnvVar::new(
        "foo",
        Utc::now().round_subsecs(6),
        &DatasetEnvVarValue::Regular("foo".to_string()),
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let new_bar_dataset_env_var = DatasetEnvVar::new(
        "bar",
        Utc::now().round_subsecs(6),
        &DatasetEnvVarValue::Regular("bar".to_string()),
        &entry_foo.id,
        SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
    )
    .unwrap();
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_dataset_env_var)
        .await;
    assert!(save_result.is_ok());
    let save_result = dataset_env_var_repo
        .upsert_dataset_env_var(&new_bar_dataset_env_var)
        .await;
    assert!(save_result.is_ok());

    let db_dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &entry_foo.id,
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        db_dataset_env_vars,
        vec![new_dataset_env_var.clone(), new_bar_dataset_env_var.clone()]
    );

    let res = dataset_entry_repo.delete_dataset_entry(&entry_foo.id).await;
    assert_matches!(res, Ok(()));

    let db_dataset_env_vars = dataset_env_var_repo
        .get_all_dataset_env_vars_by_dataset_id(
            &entry_foo.id,
            &PaginationOpts {
                offset: 0,
                limit: 5,
            },
        )
        .await
        .unwrap();

    assert!(db_dataset_env_vars.is_empty());
    let res = dataset_env_var_repo
        .get_dataset_env_var_by_id(&new_bar_dataset_env_var.id)
        .await;
    assert_matches!(res, Err(GetDatasetEnvVarError::NotFound(_)));
    let res = dataset_env_var_repo
        .get_dataset_env_var_by_key_and_dataset_id(&new_dataset_env_var.key, &entry_foo.id)
        .await;
    assert_matches!(res, Err(GetDatasetEnvVarError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
