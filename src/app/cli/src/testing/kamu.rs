// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use dill::Component;
use event_bus::EventBus;
use kamu::domain::*;
use kamu::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;
use thiserror::Error;

use crate::config::{ConfigService, CONFIG_FILENAME};
use crate::{
    accounts,
    cli,
    configure_base_catalog,
    configure_cli_catalog,
    run,
    CLIError,
    CompleteCommand,
    WorkspaceLayout,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Test wrapper on top of CLI library
pub struct Kamu {
    workspace_layout: WorkspaceLayout,
    current_account: accounts::CurrentAccountIndication,
    workspace_path: PathBuf,
    system_time: Option<DateTime<Utc>>,
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
            system_time: None,
            _temp_dir: None,
        }
    }

    pub fn set_system_time(&mut self, t: Option<DateTime<Utc>>) {
        self.system_time = t;
    }

    async fn new_workspace_tmp_inner(options: NewWorkspaceOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        if let Some(config) = options.kamu_config {
            fs::write(temp_dir.path().join(CONFIG_FILENAME), config).unwrap();
        }

        let inst = Self::new(temp_dir.path());
        let inst = Self {
            _temp_dir: Some(temp_dir),
            ..inst
        };

        let arguments = if options.is_multi_tenant {
            vec!["init", "--multi-tenant"]
        } else {
            vec!["init"]
        };

        for (env_name, env_value) in options.env_vars {
            std::env::set_var(env_name, env_value);
        }

        inst.execute(arguments).await.unwrap();

        inst
    }

    pub async fn new_workspace_tmp() -> Self {
        Self::new_workspace_tmp_inner(NewWorkspaceOptions::default()).await
    }

    pub async fn new_workspace_tmp_with(options: NewWorkspaceOptions) -> Self {
        Self::new_workspace_tmp_inner(options).await
    }

    pub fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    pub async fn assert_last_data_slice(
        &self,
        dataset_name: &DatasetName,
        expected_schema: &str,
        expected_data: &str,
    ) {
        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(self.current_account.to_current_account_subject())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
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

        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(
                vec![part_file.to_string_lossy().to_string()],
                ParquetReadOptions {
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    schema: None,
                    file_sort_order: Vec::new(),
                },
            )
            .await
            .unwrap();

        kamu_data_utils::testing::assert_data_eq(df.clone(), expected_data).await;
        kamu_data_utils::testing::assert_schema_eq(df.schema(), expected_schema);
    }

    pub async fn execute<I, S>(&self, cmd: I) -> Result<(), CommandError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let mut full_cmd: Vec<OsString> = vec!["kamu".into(), "-q".into()];

        if let Some(system_time) = &self.system_time {
            full_cmd.push("--system-time".into());
            full_cmd.push(
                system_time
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    .into(),
            );
        }

        full_cmd.extend(cmd.into_iter().map(Into::into));

        let app = cli();
        let matches = app.try_get_matches_from(&full_cmd).unwrap();

        run(self.workspace_layout.clone(), matches)
            .await
            .map_err(|e| CommandError {
                cmd: full_cmd,
                error: e,
            })
    }

    pub async fn start_api_server(self, addr: SocketAddrV4) -> Result<(), InternalError> {
        let host = addr.ip().to_string();
        let port = addr.port().to_string();

        self.execute([
            "--e2e-testing",
            "system",
            "api-server",
            "--address",
            host.as_str(),
            "--http-port",
            port.as_str(),
        ])
        .await
        .int_err()
    }

    pub fn get_server_address(&self) -> SocketAddrV4 {
        // TODO: Random port support -- this unlocks parallel running
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000)
    }

    // TODO: Generalize into execute with overridden STDOUT/ERR/IN
    pub async fn complete<S>(&self, input: S, current: usize) -> Result<Vec<String>, CLIError>
    where
        S: Into<String>,
    {
        let cli = cli();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(self.current_account.to_current_account_subject())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(self.workspace_layout.datasets_dir.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let config_service = Arc::new(ConfigService::new(&self.workspace_layout));

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
            .map(ToString::to_string)
            .collect())
    }

    pub async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) -> Result<(), CommandError> {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write_all(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()]).await
    }

    pub fn catalog(&self) -> dill::Catalog {
        let is_e2e_testing = true;
        let multi_tenant_workspace = false;
        let base_catalog = configure_base_catalog(
            &self.workspace_layout,
            multi_tenant_workspace,
            self.system_time,
            is_e2e_testing,
        )
        .build();

        let multi_tenant_workspace = true;
        let mut cli_catalog_builder = configure_cli_catalog(&base_catalog, multi_tenant_workspace);
        cli_catalog_builder.add_value(self.current_account.to_current_account_subject());
        cli_catalog_builder.build()
    }

    pub async fn get_list_of_repo_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> (Vec<String>, Vec<String>) {
        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(self.current_account.to_current_account_subject())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(self.workspace_layout.datasets_dir.clone())
                    .with_multi_tenant(false),
            )
            .add::<RemoteAliasesRegistryImpl>()
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let remote_alias_reg = catalog.get_one::<dyn RemoteAliasesRegistry>().unwrap();

        let repo_aliases = remote_alias_reg
            .get_remote_aliases(dataset_ref)
            .await
            .unwrap();
        let pull_aliases: Vec<_> = repo_aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(ToString::to_string)
            .collect();
        let push_aliases: Vec<_> = repo_aliases
            .get_by_kind(RemoteAliasKind::Push)
            .map(ToString::to_string)
            .collect();

        (pull_aliases, push_aliases)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct NewWorkspaceOptions {
    pub is_multi_tenant: bool,
    pub kamu_config: Option<String>,
    pub env_vars: Vec<(String, String)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Command {cmd:?} failed: {error}")]
pub struct CommandError {
    pub cmd: Vec<OsString>,
    pub error: CLIError,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
