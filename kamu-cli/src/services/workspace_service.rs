// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::WorkspaceLayout;
use std::path::Path;
use std::sync::Arc;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct WorkspaceService {
    workspace_layout: Arc<WorkspaceLayout>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl WorkspaceService {
    pub const LATEST_SUPPORTED_VERSION: usize = 1;

    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
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

    /// Whether there is an initialized workspace
    pub fn is_in_workspace(&self) -> bool {
        self.workspace_layout.root_dir.is_dir()
    }

    /// Whether workspace requires and upgrade
    pub fn is_upgrade_needed(&self) -> Result<bool, InternalError> {
        Ok(self.workspace_version()? != Self::LATEST_SUPPORTED_VERSION)
    }

    /// Returns the version of the current workspace
    pub fn workspace_version(&self) -> Result<usize, InternalError> {
        assert!(self.is_in_workspace());

        if !self.workspace_layout.version_path.is_file() {
            Ok(0)
        } else {
            let version_str =
                std::fs::read_to_string(&self.workspace_layout.version_path).int_err()?;

            version_str.trim().parse().int_err()
        }
    }

    /// Returns the version that code expects to function correctly
    pub fn latest_supported_version(&self) -> usize {
        Self::LATEST_SUPPORTED_VERSION
    }

    /// Perform an upgrade of the workspace if necessary
    pub fn upgrade(&self) -> Result<WorkspaceUpgradeResult, WorkspaceUpgradeError> {
        let prev_version = self.workspace_version()?;
        let mut current_version = prev_version;
        let new_version = Self::LATEST_SUPPORTED_VERSION;

        if prev_version == new_version {
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
                0 => self.upgrade_0_to_1()?,
                _ => unreachable!(),
            }

            current_version += 1;
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
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkspaceUpgradeResult {
    pub prev_version: usize,
    pub new_version: usize,
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
#[error("Workspace version {workspace_version} is newer than supported version {latest_supported_version} - upgrade to latest software version")]
pub struct WorkspaceFutureVersionError {
    pub workspace_version: usize,
    pub latest_supported_version: usize,
}
