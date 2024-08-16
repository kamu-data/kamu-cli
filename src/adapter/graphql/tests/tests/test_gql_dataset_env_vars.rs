// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::{
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    DependencyGraphServiceInMemory,
};
use kamu_core::{auth, CreateDatasetFromSnapshotUseCase, CreateDatasetResult, DatasetRepository};
use kamu_datasets::DatasetEnvVarsConfig;
use kamu_datasets_inmem::InMemoryDatasetEnvVarRepository;
use kamu_datasets_services::DatasetEnvVarServiceImpl;
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::DatasetKind;
use time_source::SystemTimeSourceDefault;

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_get_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    let mutation_code = DatasetEnvVarsHarness::create_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
        "foo_value",
        true,
    );

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "saveEnvVariable": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "listEnvVariables": {
                            "totalCount": 1,
                            "nodes": [{
                                "key": "foo",
                                "value": null,
                                "isSecret": true
                            }]
                        }
                    }
                }
            }
        })
    );

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars_with_id(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let created_dataset_env_var_id =
        json["datasets"]["byId"]["envVars"]["listEnvVariables"]["nodes"][0]["id"].clone();

    let query_code = DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
        created_dataset.dataset_handle.id.to_string().as_str(),
        created_dataset_env_var_id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "exposedValue": "foo_value"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    let mutation_code = DatasetEnvVarsHarness::create_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
        "foo_value",
        true,
    );

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "saveEnvVariable": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars_with_id(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let created_dataset_env_var_id =
        json["datasets"]["byId"]["envVars"]["listEnvVariables"]["nodes"][0]["id"].clone();

    let mutation_code = DatasetEnvVarsHarness::delete_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        &created_dataset_env_var_id.to_string(),
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "deleteEnvVariable": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    let mutation_code = DatasetEnvVarsHarness::create_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
        "foo_value",
        true,
    );

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "saveEnvVariable": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars_with_id(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let created_dataset_env_var_id =
        json["datasets"]["byId"]["envVars"]["listEnvVariables"]["nodes"][0]["id"].clone();

    let mutation_code = DatasetEnvVarsHarness::modify_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        &created_dataset_env_var_id.to_string(),
        "new_foo_value",
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "modifyEnvVariable": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "listEnvVariables": {
                            "totalCount": 1,
                            "nodes": [{
                                "key": "foo",
                                "value": "new_foo_value",
                                "isSecret": false
                            }]
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetEnvVarsHarness {
    _tempdir: tempfile::TempDir,
    _catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl DatasetEnvVarsHarness {
    async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<DummyOutboxImpl>()
                .add_value(DatasetEnvVarsConfig::sample())
                .add_builder(
                    DatasetRepositoryLocalFs::builder()
                        .with_root(datasets_dir)
                        .with_multi_tenant(false),
                )
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<SystemTimeSourceDefault>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<DependencyGraphServiceInMemory>()
                .add::<DatabaseTransactionRunner>()
                .add::<DatasetEnvVarServiceImpl>()
                .add::<InMemoryDatasetEnvVarRepository>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base).await;

        Self {
            _tempdir: tempdir,
            _catalog_anonymous: catalog_anonymous,
            catalog_authorized,
        }
    }

    async fn create_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(DatasetKind::Root)
                    .name("foo")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap()
    }

    fn get_dataset_env_vars(dataset_id: &str) -> String {
        indoc!(
            r#"
            query Datasets {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            listEnvVariables(page: 0, perPage: 5) {
                                totalCount
                                nodes {
                                    key
                                    value
                                    isSecret
                                }
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
    }

    fn get_dataset_env_vars_with_id(dataset_id: &str) -> String {
        indoc!(
            r#"
            query Datasets {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            listEnvVariables(page: 0, perPage: 5) {
                                totalCount
                                nodes {
                                    id
                                    key
                                    value
                                    isSecret
                                }
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
    }

    fn get_dataset_env_var_exposed_value(dataset_id: &str, dataset_env_var_id: &str) -> String {
        indoc!(
            r#"
            query Datasets {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            exposedValue(datasetEnvVarId: <dataset_env_var_id>)
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
        .replace("<dataset_env_var_id>", dataset_env_var_id)
    }

    fn create_dataset_env(
        dataset_id: &str,
        env_var_key: &str,
        env_var_value: &str,
        is_secret: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            saveEnvVariable(key: "<env_var_key>", value: "<env_var_value>", isSecret: <is_secret>) {
                                message
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
        .replace("<env_var_key>", env_var_key)
        .replace("<env_var_value>", env_var_value)
        .replace("<is_secret>", if is_secret { "true" } else { "false" })
    }

    fn delete_dataset_env(dataset_id: &str, env_var_id: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            deleteEnvVariable(id: <env_var_id>) {
                                message
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
        .replace("<env_var_id>", env_var_id)
    }

    fn modify_dataset_env(
        dataset_id: &str,
        env_var_id: &str,
        new_value: &str,
        is_secret: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            modifyEnvVariable(id: <env_var_id>, newValue: "<env_var_value>", isSecret: <is_secret>) {
                                message
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
        .replace("<env_var_id>", env_var_id)
        .replace("<env_var_value>", new_value)
        .replace("<is_secret>", if is_secret { "true" } else { "false" })
    }
}
