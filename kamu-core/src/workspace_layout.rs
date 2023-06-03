// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use opendatafabric::DatasetAlias;

use crate::DatasetLayout;

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
}

impl WorkspaceLayout {
    pub const VERSION: WorkspaceVersion = WorkspaceVersion::V1_WorkspaceCacheDir;

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root_dir = root.into();
        Self {
            datasets_dir: root_dir.join("datasets"),
            repos_dir: root_dir.join("repos"),
            cache_dir: root_dir.join("cache"),
            run_info_dir: root_dir.join("run"),
            version_path: root_dir.join("version"),
            root_dir,
        }
    }

    pub fn create(root: impl Into<PathBuf>) -> Result<Self, std::io::Error> {
        let ws = Self::new(root);
        if !ws.root_dir.exists() || ws.root_dir.read_dir()?.next().is_some() {
            std::fs::create_dir(&ws.root_dir)?;
        }
        std::fs::create_dir(&ws.datasets_dir)?;
        std::fs::create_dir(&ws.repos_dir)?;
        std::fs::create_dir(&ws.cache_dir)?;
        std::fs::create_dir(&ws.run_info_dir)?;
        std::fs::write(&ws.version_path, Self::VERSION.to_string())?;
        Ok(ws)
    }

    pub fn dataset_layout(&self, alias: &DatasetAlias) -> DatasetLayout {
        assert!(!alias.is_multitenant(), "Multitenancy is not yet supported");
        DatasetLayout::new(self.datasets_dir.join(&alias.dataset_name))
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
            _ => WorkspaceVersion::Unknown(value),
        }
    }
}

impl Into<u32> for WorkspaceVersion {
    fn into(self) -> u32 {
        match self {
            Self::V0_Initial => 0,
            Self::V1_WorkspaceCacheDir => 1,
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
