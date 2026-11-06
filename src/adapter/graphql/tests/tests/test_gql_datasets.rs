// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use bon::bon;
use indoc::indoc;
use kamu_accounts::*;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::*;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;

use crate::utils::{
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs_ext,
    expect_anonymous_access_error,
};

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
        assert_eq!(
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
    assert_eq!(
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
async fn test_dataset_by_id_private() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Private,
        )
        .await;

    // Can't see private dataset without logging in
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
    assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": null
            }
        }),
        res.data,
    );

    // Can see private dataset when authorized
    let res = harness
        .execute_authorized_query(
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
    assert_eq!(
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
async fn test_dataset_by_id_public() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
            odf::DatasetVisibility::Public,
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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("Foo"),
            odf::DatasetVisibility::Public,
        )
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
            .replace("<accountId>", TEST_ACCOUNT_ID.to_string().as_str()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
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
async fn test_private_dataset_listings_check_request_principal() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo = harness
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Private,
        )
        .await;
    let foo_id = &foo.dataset_handle.id;
    let none = Vec::<String>::new();

    for caller in [Caller::Anonymous, Caller::Stranger] {
        assert_eq!(
            harness.datasets_by_account_id(caller).await,
            none,
            "{caller:?}"
        );
        assert_eq!(
            harness.account_owned_datasets(caller).await,
            none,
            "{caller:?}"
        );
        assert_eq!(
            harness.datasets_by_ids(caller, foo_id).await,
            none,
            "{caller:?}"
        );
    }
    assert_eq!(
        harness.datasets_mut_by_ids(Caller::Stranger, foo_id).await,
        none
    );

    assert_eq!(harness.datasets_by_account_id(Caller::Owner).await, ["foo"]);
    assert_eq!(harness.account_owned_datasets(Caller::Owner).await, ["foo"]);
    assert_eq!(
        harness.datasets_by_ids(Caller::Owner, foo_id).await,
        ["foo"]
    );
    assert_eq!(
        harness.datasets_mut_by_ids(Caller::Owner, foo_id).await,
        ["foo"]
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

    use odf::metadata::serde::DatasetSnapshotSerializer;
    use odf::metadata::serde::yaml::YamlDatasetSnapshotSerializer;

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
    assert_eq!(
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
    assert_eq!(
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

        use odf::metadata::serde::DatasetSnapshotSerializer;
        use odf::metadata::serde::yaml::YamlDatasetSnapshotSerializer;

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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
        .await;
    let _bar_result = harness
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("bar"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
        .create_root_dataset(
            None,
            odf::DatasetName::new_unchecked("foo"),
            odf::DatasetVisibility::Public,
        )
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
    assert_eq!(
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
        &[DatasetPermission::Read],
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
        &[DatasetPermission::Read, DatasetPermission::Write],
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
            DatasetPermission::Read,
            DatasetPermission::Write,
            DatasetPermission::Maintain,
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
            DatasetPermission::Read,
            DatasetPermission::Write,
            DatasetPermission::Maintain,
            DatasetPermission::Own,
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
    granted_permissions: &[DatasetPermission],
    expected_permission: async_graphql::Value,
) {
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    let dataset_alias = odf::DatasetAlias::new(None, dataset_name.clone());

    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::SingleTenant)
        .mock_dataset_permission_checker(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from([dataset_alias]),
                )
                .make_expect_get_granted_permissions(granted_permissions, 1),
        )
        .build()
        .await;

    let foo_result = harness
        .create_root_dataset(None, dataset_name, odf::DatasetVisibility::Public)
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
    assert_eq!(
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
    catalog_no_subject: dill::Catalog,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLDatasetsHarness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_permission_checker: Option<MockDatasetPermissionChecker>,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .maybe_mock_dataset_permission_checker(mock_dataset_permission_checker)
            .build();

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<RenameDatasetUseCaseImpl>();
            b.add::<DeleteDatasetUseCaseImpl>();

            b.build()
        };

        let auth_catalogs =
            authentication_catalogs_ext(&base_catalog, None, PredefinedAccountOpts::default())
                .await;

        Self {
            base_gql_harness,
            catalog_no_subject: auth_catalogs.catalog_no_subject,
            catalog_anonymous: auth_catalogs.catalog_anonymous,
            catalog_authorized: auth_catalogs.catalog_authorized,
        }
    }

    pub async fn create_root_dataset(
        &self,
        account_name: Option<odf::AccountName>,
        name: odf::DatasetName,
        visibility: odf::DatasetVisibility,
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
                CreateDatasetUseCaseOptions {
                    dataset_visibility: visibility,
                    ..Default::default()
                },
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
        self.execute_query(query, &self.catalog_authorized).await
    }

    pub async fn execute_anonymous_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        self.execute_query(query, &self.catalog_anonymous).await
    }

    fn catalog_of(&self, caller: Caller) -> dill::Catalog {
        match caller {
            Caller::Anonymous => self.catalog_anonymous.clone(),
            Caller::Owner => self.catalog_authorized.clone(),
            Caller::Stranger => dill::CatalogBuilder::new_chained(&self.catalog_no_subject)
                .add_current_account_subject(CurrentAccountSubject::new_test_with(&"stranger"))
                .build(),
        }
    }

    pub async fn datasets_by_account_id(&self, caller: Caller) -> Vec<String> {
        let query = r#"{ datasets { byAccountId(accountId: "<id>") { nodes { name } } } }"#
            .replace("<id>", &TEST_ACCOUNT_ID.to_string());
        self.dataset_names(caller, query, &["datasets", "byAccountId", "nodes"])
            .await
    }

    pub async fn account_owned_datasets(&self, caller: Caller) -> Vec<String> {
        let query =
            r#"{ accounts { byName(name: "<name>") { ownedDatasets { nodes { name } } } } }"#
                .replace("<name>", DEFAULT_ACCOUNT_NAME_STR);
        self.dataset_names(
            caller,
            query,
            &["accounts", "byName", "ownedDatasets", "nodes"],
        )
        .await
    }

    pub async fn datasets_by_ids(&self, caller: Caller, id: &odf::DatasetID) -> Vec<String> {
        let query = r#"{ datasets { byIds(datasetIds: ["<id>"], skipMissing: true) { name } } }"#
            .replace("<id>", &id.to_string());
        self.dataset_names(caller, query, &["datasets", "byIds"])
            .await
    }

    pub async fn datasets_mut_by_ids(&self, caller: Caller, id: &odf::DatasetID) -> Vec<String> {
        let query = indoc!(
            r#"
            mutation {
                datasets { byIds(datasetIds: ["<id>"], skipMissing: true) { dataset { name } } }
            }
            "#
        )
        .replace("<id>", &id.to_string());
        let datasets_mut = self.query_json(caller, query, &["datasets", "byIds"]).await;

        datasets_mut
            .as_array()
            .unwrap()
            .iter()
            .map(|dataset_mut| dataset_mut["dataset"]["name"].as_str().unwrap().to_string())
            .collect()
    }

    /// Names of the datasets listed at `path` in the query's response
    async fn dataset_names(&self, caller: Caller, query: String, path: &[&str]) -> Vec<String> {
        self.query_json(caller, query, path)
            .await
            .as_array()
            .unwrap()
            .iter()
            .map(|dataset| dataset["name"].as_str().unwrap().to_string())
            .collect()
    }

    async fn query_json(&self, caller: Caller, query: String, path: &[&str]) -> serde_json::Value {
        let response = self.execute_query(query, &self.catalog_of(caller)).await;
        assert!(response.is_ok(), "{response:?}");

        let json = response.data.into_json().unwrap();
        path.iter().fold(&json, |value, key| &value[key]).clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum Caller {
    Anonymous,
    /// The account that owns the datasets the harness creates
    Owner,
    /// A logged account without relations to any dataset
    Stranger,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
