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
use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::DatasetEnvVarCompatServiceImpl;
use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{OutboxProvider, register_message_dispatcher};
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

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

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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

    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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

    let mutation_code = DatasetEnvVarsHarness::delete_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "var1",
        "value1",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "var2",
        "value2",
        false,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "var3",
        "value3",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
        "foo_value",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::delete_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "foo",
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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

    // Try to delete it again - should get NotFound
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "secret_var",
        "secret_value",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "secret_var",
        "new_secret_value",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "secret_var",
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "public_var",
        "public_value",
        false,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "public_var",
        "new_public_value",
        false,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "changeable_var",
        "initial_value",
        false,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_vars(
        created_dataset.dataset_handle.id.to_string().as_str(),
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code.clone()))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "changeable_var",
        "secret_value",
        true,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let query_code = DatasetEnvVarsHarness::get_dataset_env_var_exposed_value(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "changeable_var",
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(query_code))
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
    let mutation_code = DatasetEnvVarsHarness::upsert_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "my_var",
        "my_value",
        false,
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code.clone()))
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
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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
    let mutation_code = DatasetEnvVarsHarness::delete_dataset_env(
        created_dataset.dataset_handle.id.to_string().as_str(),
        "non_existent_var",
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(mutation_code))
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

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct DatasetEnvVarsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

impl DatasetEnvVarsHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add_value(DatasetEnvVarsConfig::sample())
                .add::<DatasetEnvVarCompatServiceImpl>()
                .add::<InMemoryDatasetVariableSetBindingRepository>()
                .add::<InMemoryDatasetSecretSetBindingRepository>()
                .add::<InMemoryVariableSetProjectionRepository>()
                .add::<InMemorySecretSetProjectionRepository>()
                .add::<InMemoryResourceRepository>()
                .add::<InMemoryRawResourceEventStore>();

            kamu_resources_services::register_dependencies(&mut b);
            kamu_configuration_services::register_dependencies(&mut b);

            register_message_dispatcher::<ResourceLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
            );

            b.build()
        };

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
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

    fn get_dataset_env_var_exposed_value(dataset_id: &str, dataset_env_var_key: &str) -> String {
        indoc!(
            r#"
            query Datasets {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            exposedValue(datasetEnvVarKey: "<dataset_env_var_key>")
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
        .replace("<dataset_env_var_key>", dataset_env_var_key)
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

    fn delete_dataset_env(dataset_id: &str, env_var_key: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId(datasetId: "<dataset_id>") {
                        envVars {
                            deleteEnvVariable(key: "<env_var_key>") {
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
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
