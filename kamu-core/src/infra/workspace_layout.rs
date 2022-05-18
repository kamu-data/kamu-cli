// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::infra::DatasetLayout;
use opendatafabric::DatasetName;

use std::path::PathBuf;

/// Describes the layout of the workspace on disk
#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    /// Workspace root
    pub root_dir: PathBuf,
    /// Contains datasets
    pub datasets_dir: PathBuf,
    /// Contains repository definitions
    pub repos_dir: PathBuf,
    /// Directory for storing per-run diagnostics information and logs
    pub run_info_dir: PathBuf,
}

impl WorkspaceLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root_dir = root.into();
        Self {
            datasets_dir: root_dir.join("datasets"),
            repos_dir: root_dir.join("repos"),
            run_info_dir: root_dir.join("run"),
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
        std::fs::create_dir(&ws.run_info_dir)?;
        Ok(ws)
    }

    pub fn dataset_layout(&self, name: &DatasetName) -> DatasetLayout {
        DatasetLayout::new(self.datasets_dir.join(name))
    }
}
