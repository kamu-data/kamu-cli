// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::VolumeLayout;
use opendatafabric::DatasetID;
use std::path::PathBuf;

/// Describes the layout of the dataset on disk
#[derive(Debug, Clone)]
pub struct DatasetLayout {
    /// Path to the directory containing actual data
    pub data_dir: PathBuf,
    /// Path to the checkpoints directory
    pub checkpoints_dir: PathBuf,
    /// Stores data that is not essential but can improve performance of operations like data polling
    pub cache_dir: PathBuf,
}

impl DatasetLayout {
    pub fn new(volume_layout: &VolumeLayout, dataset_id: &DatasetID) -> Self {
        Self {
            data_dir: volume_layout.data_dir.join(dataset_id),
            checkpoints_dir: volume_layout.checkpoints_dir.join(dataset_id),
            cache_dir: volume_layout.cache_dir.join(dataset_id),
        }
    }

    pub fn create(
        volume_layout: &VolumeLayout,
        dataset_id: &DatasetID,
    ) -> Result<Self, std::io::Error> {
        let dl = Self::new(volume_layout, dataset_id);
        std::fs::create_dir_all(&dl.data_dir)?;
        std::fs::create_dir_all(&dl.checkpoints_dir)?;
        std::fs::create_dir_all(&dl.cache_dir)?;
        Ok(dl)
    }
}
