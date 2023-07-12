// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use kamu::domain::*;
use kamu::{WorkspaceConfig, WorkspaceLayout, WorkspaceVersion};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct WorkspaceService {
    workspace_layout: Arc<WorkspaceLayout>,
    workspace_config: WorkspaceConfig,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl WorkspaceService {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        let workspace_config = WorkspaceService::init_workspace_config(workspace_layout.clone());

        Self {
            workspace_layout,
            workspace_config,
        }
    }

    fn init_workspace_config(workspace_layout: Arc<WorkspaceLayout>) -> WorkspaceConfig {
        if workspace_layout.config_path.is_file() {
            WorkspaceConfig::load_from(&workspace_layout.config_path).unwrap()
        } else {
            WorkspaceConfig::default()
        }
    }

    pub fn find_workspace() -> WorkspaceLayout {
        let cwd = Path::new(".").canonicalize().unwrap();
        if let Some(ws) = Self::find_workspace_rec(&cwd) {
            ws
        } else {
            WorkspaceLayout::new(cwd.join(".kamu"))
        }
    }

    fn find_workspace_rec(p: &Path) -> Option<WorkspaceLayout> {
        let root_dir = p.join(".kamu");
        if root_dir.exists() {
            Some(WorkspaceLayout::new(root_dir))
        } else if let Some(parent) = p.parent() {
            Self::find_workspace_rec(parent)
        } else {
            None
        }
    }

    //// Layout of the workspace (if we are in one)
    pub fn layout(&self) -> Option<&WorkspaceLayout> {
        if self.is_in_workspace() {
            Some(&self.workspace_layout.as_ref())
        } else {
            None
        }
    }

    /// Whether there is an initialized workspace
    pub fn is_in_workspace(&self) -> bool {
        self.workspace_layout.root_dir.is_dir()
    }

    /// Whether the workspace is multi-tenant
    pub fn is_multi_tenant_workspace(&self) -> bool {
        self.workspace_config.multi_tenant
    }

    /// Whether workspace requires and upgrade
    pub fn is_upgrade_needed(&self) -> Result<bool, InternalError> {
        Ok(self.workspace_version()? != Some(WorkspaceLayout::VERSION))
    }

    /// Returns the version of the current workspace
    pub fn workspace_version(&self) -> Result<Option<WorkspaceVersion>, InternalError> {
        if !self.is_in_workspace() {
            Ok(None)
        } else {
            if !self.workspace_layout.version_path.is_file() {
                Ok(Some(WorkspaceVersion::V0_Initial))
            } else {
                let version_str =
                    std::fs::read_to_string(&self.workspace_layout.version_path).int_err()?;

                let version: u32 = version_str.trim().parse().int_err()?;
                Ok(Some(version.into()))
            }
        }
    }

    /// Returns the version that code expects to function correctly
    pub fn latest_supported_version(&self) -> WorkspaceVersion {
        WorkspaceLayout::VERSION
    }

    /// Perform an upgrade of the workspace if necessary
    pub fn upgrade(&self) -> Result<WorkspaceUpgradeResult, WorkspaceUpgradeError> {
        let prev_version = self
            .workspace_version()?
            .expect("Upgrade called when not in workspace");

        let mut current_version = prev_version;
        let new_version = WorkspaceLayout::VERSION;

        if current_version == new_version {
            return Ok(WorkspaceUpgradeResult {
                prev_version,
                new_version,
            });
        }

        tracing::info!(%prev_version, %new_version, "Upgrading workspace");

        while current_version != new_version {
            tracing::info!(
                "Upgrading from version {} to {}",
                current_version,
                new_version
            );

            match current_version {
                WorkspaceVersion::V0_Initial => self.upgrade_0_to_1()?,
                WorkspaceVersion::V1_WorkspaceCacheDir => self.upgrade_1_to_2()?,
                _ => unreachable!(),
            }

            current_version = current_version.next();
            std::fs::write(
                &self.workspace_layout.version_path,
                current_version.to_string(),
            )
            .int_err()?;
        }

        Ok(WorkspaceUpgradeResult {
            prev_version,
            new_version,
        })
    }

    fn upgrade_0_to_1(&self) -> Result<(), InternalError> {
        for entry in self.workspace_layout.datasets_dir.read_dir().int_err()? {
            let dataset_dir = entry.int_err()?;
            let cache_dir = dataset_dir.path().join("cache");
            if cache_dir.is_dir() {
                tracing::info!(?cache_dir, "Deleting old dataset ingest cache directory");
                std::fs::remove_dir_all(cache_dir).int_err()?;
            }
        }
        let _ = std::fs::create_dir(&self.workspace_layout.cache_dir);
        Ok(())
    }

    fn upgrade_1_to_2(&self) -> Result<(), InternalError> {
        for entry in self.workspace_layout.datasets_dir.read_dir().int_err()? {
            let dataset_dir = entry.int_err()?;
            let dataset_config_path = dataset_dir.path().join("config");
            if dataset_config_path.is_file() {
                tracing::info!(
                    ?dataset_config_path,
                    "Moving DatasetConfig file from old location"
                );

                let dataset_info_dir = dataset_dir.path().join("info");
                if !dataset_info_dir.exists() {
                    std::fs::create_dir(dataset_info_dir).int_err()?;
                }

                let new_dataset_config_path = dataset_dir.path().join("info/config");
                std::fs::rename(dataset_config_path, new_dataset_config_path).int_err()?;
            }
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceUpgradeResult {
    pub prev_version: WorkspaceVersion,
    pub new_version: WorkspaceVersion,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkspaceUpgradeError {
    #[error(transparent)]
    FutureVersion(#[from] WorkspaceFutureVersionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error(
    "Workspace version {workspace_version} is newer than supported version \
     {latest_supported_version} - upgrade to latest software version"
)]
pub struct WorkspaceFutureVersionError {
    pub workspace_version: WorkspaceVersion,
    pub latest_supported_version: WorkspaceVersion,
}

/////////////////////////////////////////////////////////////////////////////////////////
