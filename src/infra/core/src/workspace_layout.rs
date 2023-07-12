// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use internal_error::{InternalError, ResultIntoInternal};
use opendatafabric::serde::yaml::Manifest;
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Consider extracting to kamu-cli layer
/// Describes the layout of the workspace on disk
#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    /// Workspace root
    pub root_dir: PathBuf,
    /// Contains datasets
    pub datasets_dir: PathBuf,
    /// Contains repository definitions
    pub repos_dir: PathBuf,
    /// Contains cached downloads and ingest checkpoints
    pub cache_dir: PathBuf,
    /// Directory for storing per-run diagnostics information and logs
    pub run_info_dir: PathBuf,
    /// Version file path
    pub version_path: PathBuf,
    /// Workspace config file path
    pub config_path: PathBuf,
}

impl WorkspaceLayout {
    pub const VERSION: WorkspaceVersion = WorkspaceVersion::V2_DatasetConfig;

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root_dir = root.into();
        Self {
            datasets_dir: root_dir.join("datasets"),
            repos_dir: root_dir.join("repos"),
            cache_dir: root_dir.join("cache"),
            run_info_dir: root_dir.join("run"),
            version_path: root_dir.join("version"),
            config_path: root_dir.join("workspace.config"),
            root_dir,
        }
    }

    pub fn create(root: impl Into<PathBuf>, multi_tenant: bool) -> Result<Self, InternalError> {
        let ws = Self::new(root);
        if !ws.root_dir.exists() || ws.root_dir.read_dir().int_err()?.next().is_some() {
            std::fs::create_dir(&ws.root_dir).int_err()?;
        }
        std::fs::create_dir(&ws.datasets_dir).int_err()?;
        std::fs::create_dir(&ws.repos_dir).int_err()?;
        std::fs::create_dir(&ws.cache_dir).int_err()?;
        std::fs::create_dir(&ws.run_info_dir).int_err()?;
        std::fs::write(&ws.version_path, Self::VERSION.to_string()).int_err()?;

        // Only save the workspace configuration if it is different from default
        let ws_config = WorkspaceConfig::new(multi_tenant);
        if ws_config != WorkspaceConfig::default() {
            ws_config.save_to(&ws.config_path).int_err()?;
        }

        Ok(ws)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Generalize this enum pattern
#[allow(non_camel_case_types)]
#[repr(u32)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceVersion {
    V0_Initial,
    V1_WorkspaceCacheDir,
    V2_DatasetConfig,
    Unknown(u32),
}

impl WorkspaceVersion {
    pub fn next(&self) -> Self {
        let v: u32 = (*self).into();
        (v + 1).into()
    }
}

impl From<u32> for WorkspaceVersion {
    fn from(value: u32) -> Self {
        match value {
            0 => WorkspaceVersion::V0_Initial,
            1 => WorkspaceVersion::V1_WorkspaceCacheDir,
            2 => WorkspaceVersion::V2_DatasetConfig,
            _ => WorkspaceVersion::Unknown(value),
        }
    }
}

impl Into<u32> for WorkspaceVersion {
    fn into(self) -> u32 {
        match self {
            Self::V0_Initial => 0,
            Self::V1_WorkspaceCacheDir => 1,
            Self::V2_DatasetConfig => 2,
            Self::Unknown(value) => value,
        }
    }
}

impl std::fmt::Display for WorkspaceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: u32 = (*self).into();
        write!(f, "{}", value)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const WORKSPACE_CONFIG_VERSION: i32 = 1;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkspaceConfig {
    pub multi_tenant: bool,
}

impl WorkspaceConfig {
    pub fn new(multi_tenant: bool) -> Self {
        Self { multi_tenant }
    }

    pub fn load_from(config_path: &Path) -> serde_yaml::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(config_path)
            .unwrap();

        let manifest: Manifest<WorkspaceConfig> = serde_yaml::from_reader(file)?;

        assert_eq!(manifest.kind, "WorkspaceConfig");
        assert_eq!(manifest.version, WORKSPACE_CONFIG_VERSION);

        Ok(manifest.content)
    }

    pub fn save_to(&self, config_path: &Path) -> serde_yaml::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(config_path)
            .unwrap();

        let manifest = Manifest {
            kind: "WorkspaceConfig".to_owned(),
            version: WORKSPACE_CONFIG_VERSION,
            content: self.clone(),
        };

        serde_yaml::to_writer(file, &manifest)
    }
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            multi_tenant: false,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
