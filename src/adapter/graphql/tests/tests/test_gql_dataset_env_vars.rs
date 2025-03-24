// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use indoc::indoc;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_inmem::*;
use kamu_datasets_services::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_get_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
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
                        "upsertEnvVariable": {
                            "message": "Created"
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

    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
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
                        "upsertEnvVariable": {
                            "message": "Created"
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

    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
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
                        "upsertEnvVariable": {
                            "message": "Created"
                        }
                    }
                }
            }
        })
    );

    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
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
                        "upsertEnvVariable": {
                            "message": "Updated"
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

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct DatasetEnvVarsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

impl DatasetEnvVarsHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add_value(DatasetEnvVarsConfig::sample())
                .add::<DatasetEnvVarServiceImpl>()
                .add::<RebacDatasetRegistryFacadeImpl>()
                .add::<InMemoryDatasetEnvVarRepository>();

            b.build()
        };

        let (_, catalog_authorized) = authentication_catalogs(&catalog_base).await;

        Self {
            base_gql_harness,
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
                    .kind(odf::DatasetKind::Root)
                    .name("foo")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
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

    fn upsert_dataset_env(
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
                            upsertEnvVariable(key: "<env_var_key>", value: "<env_var_value>", isSecret: <is_secret>) {
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
}
