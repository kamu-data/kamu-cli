// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::VolumeLayout;
use opendatafabric::{DataSlice, DatasetName, Multihash};
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
    pub fn new(volume_layout: &VolumeLayout, dataset_name: &DatasetName) -> Self {
        Self {
            data_dir: volume_layout.data_dir.join(dataset_name),
            checkpoints_dir: volume_layout.checkpoints_dir.join(dataset_name),
            cache_dir: volume_layout.cache_dir.join(dataset_name),
        }
    }

    pub fn create(
        volume_layout: &VolumeLayout,
        dataset_name: &DatasetName,
    ) -> Result<Self, std::io::Error> {
        let dl = Self::new(volume_layout, dataset_name);
        std::fs::create_dir_all(&dl.data_dir)?;
        std::fs::create_dir_all(&dl.checkpoints_dir)?;
        std::fs::create_dir_all(&dl.cache_dir)?;
        Ok(dl)
    }

    pub fn data_slice_path(&self, slice: &DataSlice) -> PathBuf {
        self.data_dir
            .join(slice.physical_hash.to_multibase_string())
    }

    pub fn checkpoint_path(&self, physical_hash: &Multihash) -> PathBuf {
        self.checkpoints_dir
            .join(physical_hash.to_multibase_string())
    }
}
