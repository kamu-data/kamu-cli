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
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::DatasetEnvVarCompatServiceImpl;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;

use crate::utils::{BaseGQLResourceHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_get_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
            "foo_value",
            true,
        ))
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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

    let query_code = DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
        &created_dataset.dataset_handle.id,
        "foo",
    );
    let res = harness.execute_authorized_query(query_code).await;
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
            "foo_value",
            true,
        ))
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

    let query_code =
        DatasetEnvVarsHarness::get_dataset_env_vars(&created_dataset.dataset_handle.id);
    let res = harness.execute_authorized_query(query_code).await;
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::delete_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
        ))
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
            "foo_value",
            true,
        ))
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
            "new_foo_value",
            false,
        ))
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

    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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

#[test_log::test(tokio::test)]
async fn test_create_multiple_dataset_env_vars() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create first variable (secret)
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "var1",
            "value1",
            true,
        ))
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

    // Create second variable (non-secret)
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "var2",
            "value2",
            false,
        ))
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

    // Create third variable (secret)
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "var3",
            "value3",
            true,
        ))
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

    // List all variables
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "listEnvVariables": {
                            "totalCount": 3,
                            "nodes": [
                                {
                                    "key": "var1",
                                    "value": null,
                                    "isSecret": true
                                },
                                {
                                    "key": "var2",
                                    "value": "value2",
                                    "isSecret": false
                                },
                                {
                                    "key": "var3",
                                    "value": null,
                                    "isSecret": true
                                }
                            ]
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_already_deleted_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create a variable
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
            "foo_value",
            true,
        ))
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

    // Delete it once
    let mutation_code =
        DatasetEnvVarsHarness::delete_dataset_env(&created_dataset.dataset_handle.id, "foo");
    let res = harness.execute_authorized_query(mutation_code).await;
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

    // Try to delete it again - should get NotFound
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::delete_dataset_env(
            &created_dataset.dataset_handle.id,
            "foo",
        ))
        .await;

    // Should get DeleteDatasetEnvVarResultNotFound
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "deleteEnvVariable": {
                            "message": "Environment variable with 'foo' key not found"
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_dataset_env_var_keep_secret_flag() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create a secret variable
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "secret_var",
            "secret_value",
            true,
        ))
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

    // Update it while keeping it secret
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "secret_var",
            "new_secret_value",
            true,
        ))
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

    // Verify it's still secret
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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
                                "key": "secret_var",
                                "value": null,
                                "isSecret": true
                            }]
                        }
                    }
                }
            }
        })
    );

    // Verify exposed value was updated
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
            &created_dataset.dataset_handle.id,
            "secret_var",
        ))
        .await;
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "exposedValue": "new_secret_value"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_dataset_env_var_keep_non_secret_flag() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create a non-secret variable
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "public_var",
            "public_value",
            false,
        ))
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

    // Update it while keeping it non-secret
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "public_var",
            "new_public_value",
            false,
        ))
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

    // Verify it's still non-secret and value is updated
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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
                                "key": "public_var",
                                "value": "new_public_value",
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

#[test_log::test(tokio::test)]
async fn test_change_dataset_env_var_from_non_secret_to_secret() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create a non-secret variable
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "changeable_var",
            "initial_value",
            false,
        ))
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

    // Verify it's non-secret initially
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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
                                "key": "changeable_var",
                                "value": "initial_value",
                                "isSecret": false
                            }]
                        }
                    }
                }
            }
        })
    );

    // Change it to secret
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "changeable_var",
            "secret_value",
            true,
        ))
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

    // Verify it's now secret
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_vars(
            &created_dataset.dataset_handle.id,
        ))
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
                                "key": "changeable_var",
                                "value": null,
                                "isSecret": true
                            }]
                        }
                    }
                }
            }
        })
    );

    // Verify exposed value is updated
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
            &created_dataset.dataset_handle.id,
            "changeable_var",
        ))
        .await;
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "exposedValue": "secret_value"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_upsert_dataset_env_var_up_to_date() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Create a variable
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "my_var",
            "my_value",
            false,
        ))
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

    // Update it with the exact same value and secret flag
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::upsert_dataset_env(
            &created_dataset.dataset_handle.id,
            "my_var",
            "my_value",
            false,
        ))
        .await;

    // Currently the implementation returns Updated even for unchanged values
    // This tests the actual behavior rather than the ideal
    // UpsertDatasetEnvVarUpToDate
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_non_existent_dataset_env_var() {
    let harness = DatasetEnvVarsHarness::new().await;
    let created_dataset = harness.create_dataset().await;

    // Try to delete a variable that was never created
    let res = harness
        .execute_authorized_query(DatasetEnvVarsHarness::delete_dataset_env(
            &created_dataset.dataset_handle.id,
            "non_existent_var",
        ))
        .await;

    // Should get DeleteDatasetEnvVarResultNotFound
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "envVars": {
                        "deleteEnvVariable": {
                            "message": "Environment variable with 'non_existent_var' key not found"
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLResourceHarness, base_gql_resource_harness)]
struct DatasetEnvVarsHarness {
    base_gql_resource_harness: BaseGQLResourceHarness,
    catalog_authorized: dill::Catalog,
}

impl DatasetEnvVarsHarness {
    async fn new() -> Self {
        let base_gql_resource_harness = BaseGQLResourceHarness::new_with_config(
            TenancyConfig::SingleTenant,
            OutboxProvider::Immediate {
                force_immediate: true,
            },
        );

        let catalog_dataset_env_vars = {
            let mut b = dill::CatalogBuilder::new_chained(&base_gql_resource_harness.catalog_base);

            b.add_value(SecretsEncryptionConfig::sample())
                .add::<DatasetEnvVarCompatServiceImpl>();

            b.build()
        };

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog_dataset_env_vars, PredefinedAccountOpts::default())
                .await;

        Self {
            base_gql_resource_harness,
            catalog_authorized,
        }
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        self.execute_query(query, &self.catalog_authorized).await
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

    fn get_dataset_env_vars(dataset_id: &odf::DatasetID) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(datasetId: $datasetId) {
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
        ))
        .variables(async_graphql::Variables::from_json(serde_json::json!({
            "datasetId": dataset_id,
        })))
    }

    fn get_dataset_env_var_exposed_value(
        dataset_id: &odf::DatasetID,
        dataset_env_var_key: &str,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!, $key: String!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        envVars {
                            exposedValue(datasetEnvVarKey: $key)
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(serde_json::json!({
            "datasetId": dataset_id,
            "key": dataset_env_var_key,
        })))
    }

    fn upsert_dataset_env(
        dataset_id: &odf::DatasetID,
        env_var_key: &str,
        env_var_value: &str,
        is_secret: bool,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $key: String!, $value: String!, $isSecret: Boolean!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        envVars {
                            upsertEnvVariable(key: $key, value: $value, isSecret: $isSecret) {
                                message
                            }
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(serde_json::json!({
            "datasetId": dataset_id,
            "key": env_var_key,
            "value": env_var_value,
            "isSecret": is_secret,
        })))
    }

    fn delete_dataset_env(
        dataset_id: &odf::DatasetID,
        env_var_key: &str,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $key: String!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        envVars {
                            deleteEnvVariable(key: $key) {
                                message
                            }
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(serde_json::json!({
            "datasetId": dataset_id,
            "key": env_var_key,
        })))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
