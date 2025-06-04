// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use kamu::ResourceLoaderImpl;
use kamu::domain::{ResourceError, ResourceLoader};
use odf::metadata::testing::MetadataFactory;
use odf::serde::DatasetSnapshotSerializer;
use odf::serde::yaml::YamlDatasetSnapshotSerializer;
use test_utils::HttpFileServer;

fn create_test_snapshot(path: &Path) -> odf::DatasetSnapshot {
    let snapshot = MetadataFactory::dataset_snapshot().name("test").build();
    let buffer = YamlDatasetSnapshotSerializer
        .write_manifest(&snapshot)
        .unwrap();
    std::fs::write(path, buffer).unwrap();
    snapshot
}

#[tokio::test]
async fn test_load_from_path() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_path(&path).await.unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader
        .load_dataset_snapshot_from_ref(path.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(expected, actual2);
}

#[tokio::test]
async fn test_load_from_file_url() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let url = url::Url::from_file_path(path.canonicalize().unwrap()).unwrap();
    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_url(&url).await.unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader
        .load_dataset_snapshot_from_ref(url.as_str())
        .await
        .unwrap();
    assert_eq!(expected, actual2);
}

#[tokio::test]
async fn test_load_from_http_url() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let http_server = HttpFileServer::new(tempdir.path()).await;
    let url = url::Url::parse(&format!("http://{}/test.yaml", http_server.local_addr())).unwrap();

    let _server_hdl = tokio::spawn(http_server.run());

    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_url(&url).await.unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader
        .load_dataset_snapshot_from_ref(url.as_str())
        .await
        .unwrap();
    assert_eq!(expected, actual2);
}

#[tokio::test]
async fn test_load_from_http_url_404() {
    let tempdir = tempfile::tempdir().unwrap();

    let http_server = HttpFileServer::new(tempdir.path()).await;
    let url = url::Url::parse(&format!("http://{}/test.yaml", http_server.local_addr())).unwrap();

    let _server_hdl = tokio::spawn(http_server.run());

    let loader = ResourceLoaderImpl::new();
    assert!(matches!(
        loader.load_dataset_snapshot_from_url(&url).await,
        Err(ResourceError::NotFound { .. })
    ));
}
