// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::*;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::serde::yaml::YamlDatasetSnapshotSerializer;
use opendatafabric::serde::DatasetSnapshotSerializer;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn dataset_by_id_does_not_exist() {
    let harness = GraphQLDatasetsHarness::new();
    let res = harness.execute_query(indoc!(
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
        .execute_query(
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

    let res = harness
        .execute_query(indoc::indoc!(
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
        ))
        .await;
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

    let res = harness
        .execute_query(
            indoc!(
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
            .replace("<content>", &snapshot_yaml.escape_default().to_string()),
        )
        .await;
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
        .execute_query(indoc!(
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

    let res = harness
        .execute_query(
            indoc!(
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
        .execute_query(
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
        .execute_query(
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

    let foo_result = harness
        .create_root_dataset(DatasetName::new_unchecked("foo"))
        .await;

    let res = harness
        .execute_query(
            indoc!(
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
        .execute_query(
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

struct GraphQLDatasetsHarness {
    _tempdir: tempfile::TempDir,
    catalog: dill::Catalog,
}

impl GraphQLDatasetsHarness {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let dataset_repo = DatasetRepositoryLocalFs::create(
            tempdir.path().join("datasets"),
            Arc::new(CurrentAccountSubject::new_test()),
            Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
            false,
        )
        .unwrap();
        let authentication_svc = kamu::testing::MockAuthenticationService::built_in();

        let cat = dill::CatalogBuilder::new()
            .add_value(dataset_repo)
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(authentication_svc)
            .bind::<dyn auth::AuthenticationService, kamu::testing::MockAuthenticationService>()
            .build();

        Self {
            _tempdir: tempdir,
            catalog: cat,
        }
    }

    pub async fn create_root_dataset(&self, name: DatasetName) -> CreateDatasetResult {
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .name(name)
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
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .name(name)
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform_aliases(vec![input_dataset.alias.clone()])
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap()
    }

    pub async fn execute_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        let patched_catalog = dill::CatalogBuilder::new_chained(&self.catalog)
            .add_value(CurrentAccountSubject::new_test())
            .build();

        kamu_adapter_graphql::schema()
            .execute(query.into().data(patched_catalog))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////
