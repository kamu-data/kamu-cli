// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::{CommandError, Kamu};
use kamu::domain::*;
use kamu::infra::{DatasetFactoryImpl, DatasetLayout, WorkspaceLayout};
use kamu::testing::{MetadataFactory, ParquetWriterHelper};
use kamu_cli::{CLIError, WorkspaceService, WorkspaceUpgradeRequired};
use opendatafabric::*;
use std::assert_matches::assert_matches;
use std::error::Error;
use std::path::Path;

async fn init_v0_workspace(workspace_path: &Path) {
    let workspace_root = workspace_path.join(".kamu");
    std::fs::create_dir(&workspace_root).unwrap();

    let datasets_dir = workspace_root.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let dataset_name = DatasetName::new_unchecked("foo");
    let dataset_dir = datasets_dir.join(&dataset_name);
    let dataset = DatasetFactoryImpl::get_local_fs(DatasetLayout::create(&dataset_dir).unwrap());

    // Metadata & refs
    dataset
        .commit_event(
            MetadataEvent::Seed(MetadataFactory::seed(DatasetKind::Root).build()),
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
            None,
            Some(OffsetInterval { start: 0, end: 10 }),
            Some(data_path),
            Some(checkpoint_path),
            None,
            None,
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
    assert_eq!(workspace_svc.workspace_version().unwrap(), Some(0));

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

    kamu.execute(["list"]).await.unwrap();
}
