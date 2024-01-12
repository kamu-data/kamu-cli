// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dill::Component;
use event_bus::EventBus;
use kamu::domain::*;
use kamu::testing::ParquetReaderHelper;
use kamu::*;
use kamu_cli::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;
use thiserror::Error;

// Test wrapper on top of CLI library
pub struct Kamu {
    workspace_layout: WorkspaceLayout,
    current_account: accounts::CurrentAccountIndication,
    workspace_path: PathBuf,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Kamu {
    pub fn new<P: Into<PathBuf>>(workspace_path: P) -> Self {
        let workspace_path = workspace_path.into();
        let workspace_layout = WorkspaceLayout::new(workspace_path.join(".kamu"));
        let specified_explicitly = false;
        let is_admin = false;
        let current_account =
            accounts::CurrentAccountIndication::new("kamu", "kamu", specified_explicitly, is_admin);
        Self {
            workspace_layout,
            current_account,
            workspace_path,
            _temp_dir: None,
        }
    }

    pub async fn new_workspace_tmp() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let inst = Self::new(temp_dir.path());
        let inst = Self {
            _temp_dir: Some(temp_dir),
            ..inst
        };

        inst.execute(["init"]).await.unwrap();

        // TODO: Remove when podman is the default
        inst.execute(["config", "set", "engine.runtime", "podman"])
            .await
            .unwrap();

        inst
    }

    pub fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    pub async fn get_last_data_slice(&self, dataset_name: &DatasetName) -> ParquetReaderHelper {
        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(self.current_account.to_current_account_subject())
            .add::<domain::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(self.workspace_layout.datasets_dir.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let dataset = dataset_repo
            .get_dataset(&dataset_name.as_local_ref())
            .await
            .unwrap();
        let slice = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.new_data)
            .try_first()
            .await
            .unwrap()
            .expect("Data slice not found");

        let part_file = kamu_data_utils::data::local_url::into_local_path(
            dataset
                .as_data_repo()
                .get_internal_url(&slice.physical_hash)
                .await,
        )
        .unwrap();

        ParquetReaderHelper::open(&part_file)
    }

    pub async fn execute<I, S>(&self, cmd: I) -> Result<(), CommandError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let mut full_cmd = vec![OsStr::new("kamu").to_owned(), OsStr::new("-q").to_owned()];
        full_cmd.extend(cmd.into_iter().map(|i| i.into()));

        let app = kamu_cli::cli();
        let matches = app.try_get_matches_from(&full_cmd).unwrap();

        kamu_cli::run(self.workspace_layout.clone(), matches)
            .await
            .map_err(|e| CommandError {
                cmd: full_cmd,
                error: e,
            })
    }

    // TODO: Generalize into execute with overridden STDOUT/ERR/IN
    pub async fn complete<S>(&self, input: S, current: usize) -> Result<Vec<String>, CLIError>
    where
        S: Into<String>,
    {
        let cli = kamu_cli::cli();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(self.current_account.to_current_account_subject())
            .add::<domain::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(self.workspace_layout.datasets_dir.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let config_service = Arc::new(kamu_cli::config::ConfigService::new(&self.workspace_layout));

        let mut buf = Vec::new();
        CompleteCommand::new(
            Some(dataset_repo),
            None,
            None,
            config_service,
            cli,
            input,
            current,
        )
        .complete(&mut buf)
        .await?;

        let output = String::from_utf8_lossy(&buf);
        Ok(output
            .trim_end()
            .split('\n')
            .map(|s| s.to_string())
            .collect())
    }

    pub async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) -> Result<(), CommandError> {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()]).await
    }

    pub fn catalog(&self) -> dill::Catalog {
        let base_catalog = kamu_cli::configure_base_catalog(&self.workspace_layout, false).build();

        let mut cli_catalog_builder = kamu_cli::configure_cli_catalog(&base_catalog);
        cli_catalog_builder.add_value(self.workspace_layout.clone());
        cli_catalog_builder.add_value(self.current_account.to_current_account_subject());
        cli_catalog_builder.build()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Command {cmd:?} failed: {error}")]
pub struct CommandError {
    pub cmd: Vec<OsString>,
    pub error: CLIError,
}
