// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use kamu_datasets::{
    DatasetEnvVarRepository,
    DatasetEnvVarService,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DatasetEnvVarsConfig,
    UpsertDatasetEnvVarStatus,
};
use kamu_datasets_inmem::InMemoryDatasetEnvVarRepository;
use kamu_datasets_services::DatasetEnvVarServiceImpl;
use secrecy::SecretString;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_upsert_dataset_env_var() {
    let harness = DatasetEnvVarServiceHarness::new();

    let create_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Secret(SecretString::from("foo_value")),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        create_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::Created,
            ..
        })
    );

    // ToDo: currently we are not checking if the secret values are equal, so we
    // are updating the same value in case they are both secret
    // The blocker is postgres implementation which requires one more additional
    // db request to fetch the value from the db and compare it with the new value
    let update_up_to_date_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Secret(SecretString::from("foo_value")),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        update_up_to_date_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::Updated,
            ..
        })
    );

    // Change visibility of env var from secret to regular
    let update_modified_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Regular("foo_value".to_owned()),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        update_modified_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::Updated,
            ..
        })
    );

    // Try to modify the regular value of the env var with the same value will
    // return up to date
    let update_modified_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Regular("foo_value".to_owned()),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        update_modified_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::UpToDate,
            ..
        })
    );

    let update_modified_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Regular("new_foo_value".to_owned()),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        update_modified_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::Updated,
            ..
        })
    );

    // Change visibility of env var back to secret
    let update_modified_result = harness
        .dataset_env_var_service
        .upsert_dataset_env_var(
            "foo_key",
            &DatasetEnvVarValue::Secret(SecretString::from("new_foo_value")),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
        )
        .await;

    assert_matches!(
        update_modified_result,
        Ok(DatasetEnvVarUpsertResult {
            status: UpsertDatasetEnvVarStatus::Updated,
            ..
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetEnvVarServiceHarness {
    _catalog: Catalog,
    dataset_env_var_service: Arc<dyn DatasetEnvVarService>,
}

impl DatasetEnvVarServiceHarness {
    fn new() -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add::<DatasetEnvVarServiceImpl>();
            b.add_value(InMemoryDatasetEnvVarRepository::new());
            b.bind::<dyn DatasetEnvVarRepository, InMemoryDatasetEnvVarRepository>();

            b.add::<SystemTimeSourceDefault>();
            b.add_value(DatasetEnvVarsConfig::sample());

            b.build()
        };

        Self {
            dataset_env_var_service: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }
}
