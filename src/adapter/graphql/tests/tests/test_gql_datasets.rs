// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bon::bon;
use indoc::indoc;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_accounts::*;
use kamu_core::auth::DatasetAction;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error, BaseGQLDatasetHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_dataset_create_empty_public {
    ($tenancy_config:expr) => {
        let harness = GraphQLDatasetsHarness::builder()
            .tenancy_config($tenancy_config)
            .build()
            .await;

        let request_code = indoc::indoc!(
            r#"
            mutation {
              datasets {
                createEmpty(datasetKind: ROOT, datasetAlias: "foo", datasetVisibility: PUBLIC) {
                  ... on CreateDatasetResultSuccess {
                    dataset {
                      name
                    }
                  }
                }
              }
            }
            "#
        );

        expect_anonymous_access_error(harness.execute_anonymous_query(request_code).await);

        let res = harness.execute_authorized_query(request_code).await;

        assert!(res.is_ok(), "{res:?}");
        pretty_assertions::assert_eq!(
            async_graphql::value!({
                "datasets": {
                    "createEmpty": {
                        "dataset": {
                            "name": "foo",
                        }
                    }
                }
            }),
            res.data,
        );
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_id_does_not_exist() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let res = harness.execute_anonymous_query(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc") {
                        name
                    }
                }
            }
            "#
        ))
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": null,
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_id() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byId (datasetId: "<id>") {
                            name
                        }
                    }
                }
                "#
            )
            .replace(
                "<id>",
                &foo_result.dataset_handle.id.as_did_str().to_stack_string(),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "name": "foo",
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_account_and_name_case_insensitive() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let account_name = odf::AccountName::new_unchecked("KaMu");

    harness
        .create_root_dataset(
            Some(account_name.clone()),
            odf::DatasetName::new_unchecked("Foo"),
        )
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kAmU", datasetName: "<name>") {
                            name,
                            owner { accountName },
                        }
                    }
                }
                "#
            )
            .replace("<name>", "FoO"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byOwnerAndName": {
                    "name": "Foo",
                    "owner": {
                       "accountName": DEFAULT_ACCOUNT_NAME_STR,
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_by_account_id() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("Foo"))
        .await;

    let res = harness
        .execute_anonymous_query(
            indoc!(
                r#"
                {
                    datasets {
                        byAccountId(accountId: "<accountId>") {
                            nodes {
                                name,
                                owner { accountName }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<accountId>", DEFAULT_ACCOUNT_ID.to_string().as_str()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byAccountId": {
                    "nodes": [
                        {
                            "name": "Foo",
                            "owner": {
                               "accountName": DEFAULT_ACCOUNT_NAME_STR,
                            }
                        }
                    ]

                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_public_st() {
    test_dataset_create_empty_public!(TenancyConfig::SingleTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_empty_public_mt() {
    test_dataset_create_empty_public!(TenancyConfig::MultiTenant);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_from_snapshot() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    use odf::metadata::serde::yaml::YamlDatasetSnapshotSerializer;
    use odf::metadata::serde::DatasetSnapshotSerializer;

    let snapshot_yaml = String::from_utf8_lossy(
        &YamlDatasetSnapshotSerializer
            .write_manifest(&snapshot)
            .unwrap(),
    )
    .to_string();

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML, datasetVisibility: PUBLIC) {
                    ... on CreateDatasetResultSuccess {
                        dataset {
                            name
                            kind
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<content>", &snapshot_yaml.escape_default().to_string());

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "createFromSnapshot": {
                    "dataset": {
                        "name": "foo",
                        "kind": "ROOT",
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_from_snapshot_malformed() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let res = harness
        .execute_authorized_query(indoc!(
            r#"
            mutation {
                datasets {
                    createFromSnapshot(snapshot: "version: 1", snapshotFormat: YAML, datasetVisibility: PUBLIC) {
                        ... on MetadataManifestMalformed {
                            __typename
                        }
                    }
                }
            }
            "#
        ))
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "createFromSnapshot": {
                    "__typename": "MetadataManifestMalformed",
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_create_from_snapshot_unauthorized() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let snapshot_yaml = {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name("another-user/foo")
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        use odf::metadata::serde::yaml::YamlDatasetSnapshotSerializer;
        use odf::metadata::serde::DatasetSnapshotSerializer;

        String::from_utf8_lossy(
            &YamlDatasetSnapshotSerializer
                .write_manifest(&snapshot)
                .unwrap(),
        )
        .to_string()
    };

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML, datasetVisibility: PUBLIC) {
                    message
                }
            }
        }
        "#
    )
    .replace("<content>", &snapshot_yaml.escape_default().to_string());

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_err(), "{res:?}");
    pretty_assertions::assert_eq!(
        [(
            vec![
                "Field(\"datasets\")".to_string(),
                "Field(\"createFromSnapshot\")".to_string()
            ],
            "Unauthorized".to_string()
        )],
        *res.errors
            .into_iter()
            .map(|e| (
                e.path
                    .into_iter()
                    .map(|p| format!("{p:?}"))
                    .collect::<Vec<_>>(),
                e.message
            ))
            .collect::<Vec<_>>(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_success() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    rename(newName: "<newName>") {
                        __typename
                        message
                        ... on RenameResultSuccess {
                            oldName
                            newName
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string())
    .replace("<newName>", "bar");

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultSuccess",
                        "message": "Success",
                        "oldName": "foo",
                        "newName": "bar"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_no_changes() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            rename(newName: "<newName>") {
                                __typename
                                message
                                ... on RenameResultNoChanges {
                                    preservedName
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string())
            .replace("<newName>", "foo"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNoChanges",
                        "message": "No changes",
                        "preservedName": "foo"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_rename_name_collision() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("bar"))
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            rename(newName: "<newName>") {
                                __typename
                                message
                                ... on RenameResultNameCollision {
                                    collidingAlias
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string())
            .replace("<newName>", "bar"),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNameCollision",
                        "message": "Dataset 'bar' already exists",
                        "collidingAlias": "bar"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_delete_success() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    delete {
                        __typename
                        message
                        ... on DeleteResultSuccess {
                            deletedDataset
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string());

    expect_anonymous_access_error(harness.execute_anonymous_query(request_code.clone()).await);

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "delete": {
                        "__typename": "DeleteResultSuccess",
                        "message": "Success",
                        "deletedDataset": "foo"
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_delete_dangling_ref() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_derived_dataset(
            odf::DatasetName::new_unchecked("bar"),
            &foo_result.dataset_handle,
        )
        .await;

    let res = harness
        .execute_authorized_query(
            indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: "<id>") {
                            delete {
                                __typename
                                message
                                ... on DeleteResultDanglingReference {
                                    notDeletedDataset
                                    danglingChildRefs
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<id>", &foo_result.dataset_handle.id.to_string()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "delete": {
                        "__typename": "DeleteResultDanglingReference",
                        "message": "Dataset 'foo' has 1 dangling reference(s)",
                        "notDeletedDataset": "foo",
                        "danglingChildRefs": ["bar"]
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_view_permissions_for_a_reader() {
    test_dataset_view_permissions(
        &[DatasetAction::Read],
        async_graphql::value!({
            "collaboration": {
                "canView": false,
                "canUpdate": false,
            },
            "envVars": {
                "canView": false,
                "canUpdate": false,
            },
            "flows": {
                "canView": true,
                "canRun": false,
            },
            "general": {
                "canRename": false,
                "canSetVisibility": false,
                "canDelete": false,
            },
            "metadata": {
                "canCommit": false,
            },
        }),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_view_permissions_for_an_editor() {
    test_dataset_view_permissions(
        &[DatasetAction::Read, DatasetAction::Write],
        async_graphql::value!({
            "collaboration": {
                "canView": false,
                "canUpdate": false,
            },
            "envVars": {
                "canView": false,
                "canUpdate": false,
            },
            "flows": {
                "canView": true,
                "canRun": false,
            },
            "general": {
                "canRename": false,
                "canSetVisibility": false,
                "canDelete": false,
            },
            "metadata": {
                "canCommit": true,
            },
        }),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_view_permissions_for_a_maintainer() {
    test_dataset_view_permissions(
        &[
            DatasetAction::Read,
            DatasetAction::Write,
            DatasetAction::Maintain,
        ],
        async_graphql::value!({
            "collaboration": {
                "canView": true,
                "canUpdate": true,
            },
            "envVars": {
                "canView": true,
                "canUpdate": true,
            },
            "flows": {
                "canView": true,
                "canRun": true,
            },
            "general": {
                "canRename": true,
                "canSetVisibility": true,
                "canDelete": false,
            },
            "metadata": {
                "canCommit": true,
            },
        }),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_view_permissions_for_an_owner() {
    test_dataset_view_permissions(
        &[
            DatasetAction::Read,
            DatasetAction::Write,
            DatasetAction::Maintain,
            DatasetAction::Own,
        ],
        async_graphql::value!({
            "collaboration": {
                "canView": true,
                "canUpdate": true,
            },
            "envVars": {
                "canView": true,
                "canUpdate": true,
            },
            "flows": {
                "canView": true,
                "canRun": true,
            },
            "general": {
                "canRename": true,
                "canSetVisibility": true,
                "canDelete": true,
            },
            "metadata": {
                "canCommit": true,
            },
        }),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_view_permissions(
    allowed_actions: &[DatasetAction],
    expected_permission: async_graphql::Value,
) {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .mock_dataset_action_authorizer(
            MockDatasetActionAuthorizer::new()
                .expect_check_read_a_dataset(1, true)
                .make_expect_get_allowed_actions(allowed_actions, 1),
        )
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, odf::DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        query {
            datasets {
                byId (datasetId: "<id>") {
                    permissions {
                        collaboration {
                            canView
                            canUpdate
                        }
                        envVars {
                            canView
                            canUpdate
                        }
                        flows {
                            canView
                            canRun
                        }
                        general {
                            canRename
                            canSetVisibility
                            canDelete
                        }
                        metadata {
                            canCommit
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string());

    let res = harness.execute_authorized_query(request_code).await;

    assert!(res.is_ok(), "{res:?}");
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "permissions": expected_permission
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLDatasetsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLDatasetsHarness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .build();

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<RenameDatasetUseCaseImpl>();
            b.add::<DeleteDatasetUseCaseImpl>();

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog).await;

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub async fn create_root_dataset(
        &self,
        account_name: Option<odf::AccountName>,
        name: odf::DatasetName,
    ) -> CreateDatasetResult {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(account_name, name))
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        name: odf::DatasetName,
        input_dataset: &odf::DatasetHandle,
    ) -> CreateDatasetResult {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(None, name))
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(vec![input_dataset.alias.clone()])
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_authorized.clone()))
            .await
    }

    pub async fn execute_anonymous_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_anonymous.clone()))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
