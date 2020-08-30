use crate::utils::HttpServer;
use kamu::domain::{ResourceError, ResourceLoader};
use kamu::infra::serde::yaml::*;
use kamu::infra::ResourceLoaderImpl;
use kamu_test::*;

use std::path::Path;

fn create_test_snapshot(path: &Path) -> DatasetSnapshot {
    let snapshot = MetadataFactory::dataset_snapshot().id("test").build();
    let manifest = Manifest {
        api_version: 1,
        kind: "DatasetSnapshot".to_owned(),
        content: snapshot,
    };

    let file = std::fs::File::create(path).unwrap();
    serde_yaml::to_writer(file, &manifest).unwrap();

    manifest.content
}

#[test]
fn test_load_from_path() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_path(&path).unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader
        .load_dataset_snapshot_from_ref(path.to_str().unwrap())
        .unwrap();
    assert_eq!(expected, actual2);
}

#[test]
fn test_load_from_file_url() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let url = url::Url::from_file_path(&path.canonicalize().unwrap()).unwrap();
    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_url(&url).unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader.load_dataset_snapshot_from_ref(url.as_str()).unwrap();
    assert_eq!(expected, actual2);
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_load_from_http_url() {
    let tempdir = tempfile::tempdir().unwrap();

    let path = tempdir.path().join("test.yaml");
    let expected = create_test_snapshot(&path);

    let http_server = HttpServer::new(tempdir.path());
    let url = url::Url::parse(&format!(
        "http://{}:{}/test.yaml",
        http_server.address,
        http_server.host_port,
    ))
    .unwrap();

    let loader = ResourceLoaderImpl::new();
    let actual = loader.load_dataset_snapshot_from_url(&url).unwrap();
    assert_eq!(expected, actual);

    let actual2 = loader.load_dataset_snapshot_from_ref(url.as_str()).unwrap();
    assert_eq!(expected, actual2);
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_load_from_http_url_404() {
    let tempdir = tempfile::tempdir().unwrap();

    let http_server = HttpServer::new(tempdir.path());
    let url = url::Url::parse(&format!(
        "http://{}:{}/test.yaml",
        http_server.address,
        http_server.host_port,
    ))
    .unwrap();

    let loader = ResourceLoaderImpl::new();
    assert!(matches!(
        loader.load_dataset_snapshot_from_url(&url),
        Err(ResourceError::NotFound { .. })
    ));
}
