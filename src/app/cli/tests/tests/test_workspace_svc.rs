// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::error::Error;
use std::path::Path;

use event_bus::EventBus;
use kamu::domain::*;
use kamu::testing::{MetadataFactory, ParquetWriterHelper};
use kamu::*;
use kamu_cli::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use crate::utils::{CommandError, Kamu};

async fn init_v0_workspace(workspace_path: &Path) {
    let workspace_root = workspace_path.join(".kamu");
    std::fs::create_dir(&workspace_root).unwrap();

    let datasets_dir = workspace_root.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let dataset_name = DatasetName::new_unchecked("foo");
    let dataset_dir = datasets_dir.join(&dataset_name);

    let catalog = dill::CatalogBuilder::new().add::<EventBus>().build();
    let dataset = DatasetFactoryImpl::get_local_fs(
        DatasetLayout::create(&dataset_dir).unwrap(),
        catalog.get_one().unwrap(),
    );

    // Metadata & refs
    dataset
        .commit_event(
            MetadataFactory::seed(DatasetKind::Root).build().into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    dataset
        .commit_event(
            MetadataFactory::set_data_schema().build().into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    // Data & checkpoints
    let data_path = workspace_path.join("tmp_data");
    ParquetWriterHelper::from_sample_data(&data_path).unwrap();

    let checkpoint_path = workspace_path.join("tmp_checkpoint");
    std::fs::write(&checkpoint_path, b"checkpoint").unwrap();

    dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(OffsetInterval { start: 0, end: 10 }),
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(data_path)),
            Some(OwnedFile::new(checkpoint_path)),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    // Summary
    dataset
        .get_summary(GetSummaryOpts::default())
        .await
        .unwrap();

    // Ingest cache directory
    let ingest_cache_dir = dataset_dir.join("cache");
    std::fs::create_dir(&ingest_cache_dir).unwrap();

    std::fs::write(ingest_cache_dir.join("fetch.yaml"), b"<fetch.yaml>").unwrap();
    std::fs::write(ingest_cache_dir.join("commit.yaml"), b"<commit.yaml>").unwrap();

    // Dataset config
    let dataset_config = DatasetConfig::default();
    let manifest = Manifest {
        kind: "DatasetConfig".to_owned(),
        version: 1,
        content: dataset_config,
    };
    let v0_config_path = dataset_dir.join("config");
    let file = std::fs::File::create(v0_config_path).unwrap();
    serde_yaml::to_writer(file, &manifest).unwrap();
}

#[test_log::test(tokio::test)]
async fn test_workspace_upgrade() {
    let temp_dir = tempfile::tempdir().unwrap();

    let kamu = Kamu::new(temp_dir.path());
    let workspace_svc = kamu.catalog().get_one::<WorkspaceService>().unwrap();
    assert_eq!(workspace_svc.workspace_version().unwrap(), None);

    init_v0_workspace(temp_dir.path()).await;

    assert!(!temp_dir.path().join(".kamu/version").is_file());
    assert!(temp_dir.path().join(".kamu/datasets/foo/cache").is_dir());
    assert!(temp_dir.path().join(".kamu/datasets/foo/config").is_file());
    assert!(!temp_dir
        .path()
        .join(".kamu/datasets/foo/info/config")
        .is_file());
    assert_eq!(
        workspace_svc.workspace_version().unwrap(),
        Some(WorkspaceVersion::V0_Initial)
    );

    assert_matches!(
        kamu.execute(["list"]).await,
        Err(CommandError {
            error: CLIError::UsageError(err),
            ..
        }) if err.source().unwrap().is::<WorkspaceUpgradeRequired>()
    );

    kamu.execute(["system", "upgrade-workspace"]).await.unwrap();

    assert_eq!(
        workspace_svc.workspace_version().unwrap(),
        Some(WorkspaceLayout::VERSION)
    );

    assert!(temp_dir.path().join(".kamu/version").is_file());
    assert!(!temp_dir.path().join(".kamu/datasets/foo/cache").is_dir());
    assert!(!temp_dir.path().join(".kamu/datasets/foo/config").is_file());
    assert!(temp_dir
        .path()
        .join(".kamu/datasets/foo/info/config")
        .is_file());

    kamu.execute(["list"]).await.unwrap();
}
