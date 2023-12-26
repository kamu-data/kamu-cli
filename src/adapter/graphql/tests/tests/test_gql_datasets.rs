// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::serde::yaml::YamlDatasetSnapshotSerializer;
use opendatafabric::serde::DatasetSnapshotSerializer;
use opendatafabric::*;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_by_id_does_not_exist() {
    let harness = GraphQLDatasetsHarness::new();
    let res = harness.execute_anonymous_query(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:z4k88e8n8Je6fC9Lz9FHrZ7XGsikEyBwTwtMBzxp4RH9pbWn4UM") {
                        name
                    }
                }
            }
            "#
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": null,
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_by_id() {
    let harness = GraphQLDatasetsHarness::new();

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
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
            .replace("<id>", &foo_result.dataset_handle.id.to_string()),
        )
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "name": "foo",
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_create_empty() {
    let harness = GraphQLDatasetsHarness::new();

    let request_code = indoc::indoc!(
        r#"
        mutation {
            datasets {
                createEmpty (datasetKind: ROOT, datasetName: "foo") {
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "createEmpty": {
                    "dataset": {
                        "name": "foo",
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_create_from_snapshot() {
    let harness = GraphQLDatasetsHarness::new();

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

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
                createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML) {
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "createFromSnapshot": {
                    "dataset": {
                        "name": "foo",
                        "kind": "ROOT",
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_create_from_snapshot_malformed() {
    let harness = GraphQLDatasetsHarness::new();

    let res = harness
        .execute_authorized_query(indoc!(
            r#"
        mutation {
            datasets {
                createFromSnapshot(snapshot: "version: 1", snapshotFormat: YAML) {
                    ... on MetadataManifestMalformed {
                        __typename
                    }
                }
            }
        }
        "#
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "createFromSnapshot": {
                    "__typename": "MetadataManifestMalformed",
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_rename_success() {
    let harness = GraphQLDatasetsHarness::new();

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
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
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_rename_no_changes() {
    let harness = GraphQLDatasetsHarness::new();

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNoChanges",
                        "message": "No changes",
                        "preservedName": "foo"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_rename_name_collision() {
    let harness = GraphQLDatasetsHarness::new();

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_root_dataset(DatasetName::new_unchecked("bar"))
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "rename": {
                        "__typename": "RenameResultNameCollision",
                        "message": "Dataset 'bar' already exists",
                        "collidingAlias": "bar"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_delete_success() {
    let harness = GraphQLDatasetsHarness::new();
    harness.init_dependencies_graph().await;

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "delete": {
                        "__typename": "DeleteResultSuccess",
                        "message": "Success",
                        "deletedDataset": "foo"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_delete_dangling_ref() {
    let harness = GraphQLDatasetsHarness::new();
    harness.init_dependencies_graph().await;

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
        .await;
    let _bar_result = harness
        .create_derived_dataset(
            DatasetName::new_unchecked("bar"),
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
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
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
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_view_permissions() {
    let harness = GraphQLDatasetsHarness::new();

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
        .await;

    let request_code = indoc!(
        r#"
        query {
            datasets {
                byId (datasetId: "<id>") {
                    permissions {
                        canView
                        canDelete
                        canRename
                        canCommit
                        canSchedule
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &foo_result.dataset_handle.id.to_string());

    let res = harness.execute_authorized_query(request_code).await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "permissions": {
                        "canView": true,
                        "canDelete": true,
                        "canRename": true,
                        "canCommit": true,
                        "canSchedule": true,
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLDatasetsHarness {
    _tempdir: tempfile::TempDir,
    base_catalog: dill::Catalog,
    catalog_authorized: dill::Catalog,
    catalog_anonymous: dill::Catalog,
}

impl GraphQLDatasetsHarness {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let base_catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(kamu::testing::MockAuthenticationService::built_in())
            .bind::<dyn auth::AuthenticationService, kamu::testing::MockAuthenticationService>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .build();

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog);

        Self {
            _tempdir: tempdir,
            base_catalog,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub async fn init_dependencies_graph(&self) {
        let dataset_repo = self
            .catalog_authorized
            .get_one::<dyn DatasetRepository>()
            .unwrap();
        let dependency_graph_service = self
            .base_catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        dependency_graph_service
            .eager_initialization(&DependencyGraphRepositoryInMemory::new(dataset_repo))
            .await
            .unwrap();
    }

    pub async fn create_root_dataset(&self, name: DatasetName) -> CreateDatasetResult {
        let dataset_repo = self
            .catalog_authorized
            .get_one::<dyn DatasetRepository>()
            .unwrap();
        dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(None, name))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        name: DatasetName,
        input_dataset: &DatasetHandle,
    ) -> CreateDatasetResult {
        let dataset_repo = self
            .catalog_authorized
            .get_one::<dyn DatasetRepository>()
            .unwrap();
        dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(None, name))
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(vec![input_dataset.alias.clone()])
                            .build(),
                    )
                    .build(),
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

////////////////////////////////////////////////////////////////////////////////////////
