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
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use kamu_resources::ResourceID;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;

use crate::utils::{BaseGQLResourceHarness, PredefinedAccountOpts, authentication_catalogs_ext};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_variable_set_bindings_success() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let resource_id = harness.create_variable_set_resource("my-varset").await;
    let resource_id_str = resource_id.to_string();

    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[&resource_id_str]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let res = harness
        .execute_authorized_query(harness.query_variable_set_bindings(&dataset.dataset_handle.id))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "variableSetBindings": [
                            {
                                "resourceId": resource_id_str,
                                "bindingOrder": 0,
                                "resourceName": "my-varset",
                                "resourceKind": { "value": "VariableSet" },
                                "apiVersion": "kamu.dev/v1alpha1"
                            }
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_secret_set_bindings_success() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let resource_id = harness.create_secret_set_resource("my-secretset").await;
    let resource_id_str = resource_id.to_string();

    let res = harness
        .execute_authorized_query(
            harness.replace_secret_set_bindings(&dataset.dataset_handle.id, &[&resource_id_str]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceSecretSetBindings": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let res = harness
        .execute_authorized_query(harness.query_secret_set_bindings(&dataset.dataset_handle.id))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "secretSetBindings": [
                            {
                                "resourceId": resource_id_str,
                                "bindingOrder": 0,
                                "resourceName": "my-secretset",
                                "resourceKind": { "value": "SecretSet" },
                                "apiVersion": "kamu.dev/v1alpha1"
                            }
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_bindings_empty_list() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let resource_id = harness.create_variable_set_resource("some-varset").await;
    let resource_id_str = resource_id.to_string();

    // Set a binding first
    harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[&resource_id_str]),
        )
        .await;

    // Now clear it with empty list
    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": "Success"
                        }
                    }
                }
            }
        })
    );

    let res = harness
        .execute_authorized_query(harness.query_variable_set_bindings(&dataset.dataset_handle.id))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "variableSetBindings": []
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_bindings_resource_not_found() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let nonexistent_id = ResourceID::new(uuid::Uuid::new_v4()).to_string();

    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[&nonexistent_id]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": format!("Resource '{nonexistent_id}' not found")
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_variable_set_bindings_wrong_kind() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    // Create a SecretSet resource but try to bind it as a VariableSet
    let resource_id = harness
        .create_secret_set_resource("wrong-kind-secret")
        .await;
    let resource_id_str = resource_id.to_string();

    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[&resource_id_str]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": format!(
                                "Resource '{resource_id_str}' has kind 'SecretSet' but expected 'VariableSet'"
                            )
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_bindings_wrong_account() {
    // Resource is owned by the default test account; we try to bind it
    // from another account (owner of the dataset), which should yield
    // ResourceAccountMismatch.
    let harness = DatasetConfigurationHarness::new_for_account("other-account").await;
    let dataset = harness.create_dataset().await;

    // Resource was created under the *default* test account, not "other-account"
    let resource_id = harness
        .create_variable_set_resource_for_default_account("foreign-varset")
        .await;
    let resource_id_str = resource_id.to_string();

    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[&resource_id_str]),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": format!(
                                "Resource '{resource_id_str}' does not belong to the current account"
                            )
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_replace_bindings_duplicate_refs() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let resource_id = harness.create_variable_set_resource("dup-varset").await;
    let resource_id_str = resource_id.to_string();

    let res = harness
        .execute_authorized_query(harness.replace_variable_set_bindings(
            &dataset.dataset_handle.id,
            &[&resource_id_str, &resource_id_str],
        ))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "replaceVariableSetBindings": {
                            "message": format!("Duplicate resource '{resource_id_str}' in the binding list")
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_query_bindings_ordered() {
    let harness = DatasetConfigurationHarness::new().await;
    let dataset = harness.create_dataset().await;

    let id_a = harness.create_variable_set_resource("varset-alpha").await;
    let id_b = harness.create_variable_set_resource("varset-beta").await;
    let id_a_str = id_a.to_string();
    let id_b_str = id_b.to_string();

    harness
        .execute_authorized_query(
            harness
                .replace_variable_set_bindings(&dataset.dataset_handle.id, &[&id_a_str, &id_b_str]),
        )
        .await;

    let res = harness
        .execute_authorized_query(harness.query_variable_set_bindings(&dataset.dataset_handle.id))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "configuration": {
                        "variableSetBindings": [
                            {
                                "resourceId": id_a,
                                "bindingOrder": 0,
                                "resourceName": "varset-alpha",
                                "resourceKind": { "value": "VariableSet" },
                                "apiVersion": "kamu.dev/v1alpha1"
                            },
                            {
                                "resourceId": id_b,
                                "bindingOrder": 1,
                                "resourceName": "varset-beta",
                                "resourceKind": { "value": "VariableSet" },
                                "apiVersion": "kamu.dev/v1alpha1"
                            }
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Permission tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_configuration_requires_owner_access_for_query() {
    // A user with Maintain but not Own access must get "Dataset access error"
    // when querying the configuration sub-object.
    //
    // The query path resolves byId via the DataLoader, which calls
    // classify_dataset_handles_by_allowance(Read) before the Own check fires.
    let dataset_alias =
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("test-dataset"));
    let mock = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            1,
            std::collections::HashSet::from([dataset_alias]),
        )
        .make_expect_get_allowed_actions(
            &[
                DatasetAction::Read,
                DatasetAction::Write,
                DatasetAction::Maintain,
            ],
            1,
        );
    let harness = DatasetConfigurationHarness::new_with_mock(mock).await;
    let dataset = harness.create_dataset().await;

    let res = harness
        .execute_authorized_query(harness.query_variable_set_bindings(&dataset.dataset_handle.id))
        .await;

    assert!(res.is_err(), "{res:#?}");
    assert_eq!(
        vec!["Dataset access error".to_string()],
        res.errors
            .into_iter()
            .map(|e| e.message)
            .collect::<Vec<_>>(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_configuration_requires_owner_access_for_mutation() {
    // A user with Maintain but not Own access must get "Dataset access error"
    // when calling replaceVariableSetBindings.
    //
    // The mutation byId resolver checks Write access via get_allowed_actions
    // directly (no DataLoader classify step), then configuration checks for Own.
    let mock = MockDatasetActionAuthorizer::new().make_expect_get_allowed_actions(
        &[
            DatasetAction::Read,
            DatasetAction::Write,
            DatasetAction::Maintain,
        ],
        1,
    );
    let harness = DatasetConfigurationHarness::new_with_mock(mock).await;
    let dataset = harness.create_dataset().await;

    let res = harness
        .execute_authorized_query(
            harness.replace_variable_set_bindings(&dataset.dataset_handle.id, &[]),
        )
        .await;

    assert!(res.is_err(), "{res:#?}");
    assert_eq!(
        vec!["Dataset access error".to_string()],
        res.errors
            .into_iter()
            .map(|e| e.message)
            .collect::<Vec<_>>(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLResourceHarness, base_gql_resource_harness)]
struct DatasetConfigurationHarness {
    base_gql_resource_harness: BaseGQLResourceHarness,
    catalog_authorized: dill::Catalog,
    main_account_id: odf::AccountID,
}

impl DatasetConfigurationHarness {
    async fn new() -> Self {
        Self::new_internal(None, None).await
    }

    async fn new_for_account(account_name: &str) -> Self {
        let subject = kamu_accounts::CurrentAccountSubject::new_test_with(&account_name);
        Self::new_internal(Some(subject), None).await
    }

    async fn new_with_mock(mock: MockDatasetActionAuthorizer) -> Self {
        Self::new_internal(None, Some(mock)).await
    }

    async fn new_internal(
        subject: Option<kamu_accounts::CurrentAccountSubject>,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        let base_gql_resource_harness = if let Some(mock) = mock_dataset_action_authorizer {
            let base_gql_harness = crate::utils::BaseGQLDatasetHarness::builder()
                .tenancy_config(TenancyConfig::SingleTenant)
                .outbox_provider(OutboxProvider::Immediate {
                    force_immediate: true,
                })
                .mock_dataset_action_authorizer(mock)
                .build();

            let catalog_base =
                BaseGQLResourceHarness::make_base_gql_resource_catalog(base_gql_harness.catalog());

            BaseGQLResourceHarness::new(base_gql_harness, catalog_base)
        } else {
            BaseGQLResourceHarness::new_with_config(
                TenancyConfig::SingleTenant,
                OutboxProvider::Immediate {
                    force_immediate: true,
                },
            )
        };

        let result = authentication_catalogs_ext(
            &base_gql_resource_harness.catalog_base,
            subject,
            PredefinedAccountOpts::default(),
        )
        .await;

        Self {
            base_gql_resource_harness,
            main_account_id: result
                .catalog_authorized
                .get_one::<kamu_accounts::CurrentAccountSubject>()
                .unwrap()
                .account_id()
                .clone(),
            catalog_authorized: result.catalog_authorized,
        }
    }

    async fn execute_authorized_query(
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
                    .name("test-dataset")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_variable_set_resource(&self, name: &str) -> ResourceID {
        BaseGQLResourceHarness::create_variable_set_resource(
            &self.catalog_authorized,
            &self.main_account_id,
            name,
            BaseGQLResourceHarness::dummy_variable_set_spec(),
        )
        .await
    }

    async fn create_secret_set_resource(&self, name: &str) -> ResourceID {
        BaseGQLResourceHarness::create_secret_set_resource(
            &self.catalog_authorized,
            &self.main_account_id,
            name,
            BaseGQLResourceHarness::dummy_secret_set_spec(),
        )
        .await
    }

    // Creates a VariableSet resource owned by the default test account (not the
    // current "authorized" account), to simulate a foreign resource.
    async fn create_variable_set_resource_for_default_account(&self, name: &str) -> ResourceID {
        let account_id = kamu_accounts::CurrentAccountSubject::new_test()
            .account_id()
            .clone();
        BaseGQLResourceHarness::create_variable_set_resource(
            &self.catalog_authorized,
            &account_id,
            name,
            BaseGQLResourceHarness::dummy_variable_set_spec(),
        )
        .await
    }

    fn replace_variable_set_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        resource_ids: &[&str],
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $resourceIds: [String!]!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        configuration {
                            replaceVariableSetBindings(resourceIds: $resourceIds) {
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
            "resourceIds": resource_ids,
        })))
    }

    fn replace_secret_set_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        resource_ids: &[&str],
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $resourceIds: [String!]!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        configuration {
                            replaceSecretSetBindings(resourceIds: $resourceIds) {
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
            "resourceIds": resource_ids,
        })))
    }

    fn query_variable_set_bindings(&self, dataset_id: &odf::DatasetID) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        configuration {
                            variableSetBindings {
                                resourceId
                                bindingOrder
                                resourceName
                                resourceKind { value }
                                apiVersion
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

    fn query_secret_set_bindings(&self, dataset_id: &odf::DatasetID) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        configuration {
                            secretSetBindings {
                                resourceId
                                bindingOrder
                                resourceName
                                resourceKind { value }
                                apiVersion
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
