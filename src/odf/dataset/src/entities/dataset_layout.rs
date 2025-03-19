// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

/// Describes the layout of the dataset on disk
#[derive(Debug, Clone)]
pub struct DatasetLayout {
    /// Top-level dataset directory
    pub root_dir: PathBuf,
    /// Directory containing the metadata chain
    pub blocks_dir: PathBuf,
    /// Directory containing the named block references
    pub refs_dir: PathBuf,
    /// Directory containing the data part files
    pub data_dir: PathBuf,
    /// Directory containing the checkpoint files
    pub checkpoints_dir: PathBuf,
    /// Directory containing auxiliary information (e.g. summary, lookup tables
    /// etc.)
    pub info_dir: PathBuf,
}

impl DatasetLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root_dir = root.into();
        Self {
            blocks_dir: root_dir.join("blocks"),
            refs_dir: root_dir.join("refs"),
            data_dir: root_dir.join("data"),
            checkpoints_dir: root_dir.join("checkpoints"),
            info_dir: root_dir.join("info"),
            root_dir,
        }
    }

    pub fn create(root: impl Into<PathBuf>) -> Result<Self, std::io::Error> {
        let dl = Self::new(root);
        // Use `create_dir_all`, as it ignores if the directories already exist
        std::fs::create_dir_all(&dl.blocks_dir)?;
        std::fs::create_dir_all(&dl.refs_dir)?;
        std::fs::create_dir_all(&dl.data_dir)?;
        std::fs::create_dir_all(&dl.checkpoints_dir)?;
        std::fs::create_dir_all(&dl.info_dir)?;
        Ok(dl)
    }
}
