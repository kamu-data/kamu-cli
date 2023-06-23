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

#[test_log::test(tokio::test)]
async fn dataset_by_id_does_not_exist() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = DatasetRepositoryLocalFs::new(workspace_layout.datasets_dir.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc!(
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

#[test_log::test(tokio::test)]
async fn dataset_by_id() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = DatasetRepositoryLocalFs::new(workspace_layout.datasets_dir.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let local_repo = cat.get_one::<dyn DatasetRepository>().unwrap();
    let create_result = local_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(
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
            .replace("<id>", &create_result.dataset_handle.id.to_string()),
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

#[test_log::test(tokio::test)]
async fn dataset_create_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = DatasetRepositoryLocalFs::new(workspace_layout.datasets_dir.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc::indoc!(
            r#"
            {
                datasets {
                    createEmpty (accountId: "kamu", datasetKind: ROOT, datasetName: "foo") {
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

#[test_log::test(tokio::test)]
async fn dataset_create_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = DatasetRepositoryLocalFs::new(workspace_layout.datasets_dir.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

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

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc!(
            r#"
            {
                datasets {
                    createFromSnapshot (accountId: "kamu", snapshot: "<content>", snapshotFormat: YAML) {
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
        ).replace("<content>", &snapshot_yaml.escape_default().to_string()))
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

#[test_log::test(tokio::test)]
async fn dataset_create_from_snapshot_malformed() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = DatasetRepositoryLocalFs::new(workspace_layout.datasets_dir.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc!(
            r#"
            {
                datasets {
                    createFromSnapshot (accountId: "kamu", snapshot: "version: 1", snapshotFormat: YAML) {
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
