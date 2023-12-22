// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use kamu_core::*;
use opendatafabric::*;

use super::ParquetWriterHelper;
use crate::DatasetLayout;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetTestHelper {}

impl DatasetTestHelper {
    pub fn assert_datasets_in_sync(
        dataset_1_layout: &DatasetLayout,
        dataset_2_layout: &DatasetLayout,
    ) {
        assert!(dataset_1_layout.blocks_dir.exists());
        assert!(dataset_2_layout.blocks_dir.exists());

        assert_eq!(
            DatasetTestHelper::list_files(&dataset_1_layout.blocks_dir),
            DatasetTestHelper::list_files(&dataset_2_layout.blocks_dir)
        );
        assert_eq!(
            DatasetTestHelper::list_files(&dataset_1_layout.refs_dir),
            DatasetTestHelper::list_files(&dataset_2_layout.refs_dir)
        );
        assert_eq!(
            DatasetTestHelper::list_files(&dataset_1_layout.data_dir),
            DatasetTestHelper::list_files(&dataset_2_layout.data_dir)
        );
        assert_eq!(
            DatasetTestHelper::list_files(&dataset_1_layout.checkpoints_dir),
            DatasetTestHelper::list_files(&dataset_2_layout.checkpoints_dir)
        );

        let head_1 = std::fs::read_to_string(dataset_1_layout.refs_dir.join("head")).unwrap();
        let head_2 = std::fs::read_to_string(dataset_2_layout.refs_dir.join("head")).unwrap();
        assert_eq!(head_1, head_2);
    }

    fn list_files(dir: &Path) -> Vec<PathBuf> {
        if !dir.exists() {
            return Vec::new();
        }

        let mut v = DatasetTestHelper::_list_files_rec(dir);

        for path in v.iter_mut() {
            *path = path.strip_prefix(dir).unwrap().to_owned();
        }

        v.sort();
        v
    }

    fn _list_files_rec(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .flat_map(|e| {
                let entry = e.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    DatasetTestHelper::_list_files_rec(&path)
                } else {
                    vec![path]
                }
            })
            .collect()
    }

    pub async fn append_random_data(
        dataset_repo: &dyn DatasetRepository,
        dataset_ref: impl Into<DatasetRef>,
    ) -> Multihash {
        let tmp_dir = tempfile::tempdir().unwrap();

        let ds = dataset_repo.get_dataset(&dataset_ref.into()).await.unwrap();

        let prev_data = ds
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| match b.event {
                MetadataEvent::AddData(e) => Some(e),
                _ => None,
            })
            .try_first()
            .await
            .unwrap();

        let data_path = tmp_dir.path().join("data");
        let checkpoint_path = tmp_dir.path().join("checkpoint");
        ParquetWriterHelper::from_sample_data(&data_path).unwrap();

        use rand::RngCore;

        let mut data = [0u8; 1048576];
        rand::thread_rng().fill_bytes(&mut data);

        std::fs::write(&checkpoint_path, data).unwrap();

        let input_checkpoint = prev_data
            .as_ref()
            .and_then(|e| e.output_checkpoint.as_ref())
            .map(|c| c.physical_hash.clone());

        let prev_offset = prev_data
            .as_ref()
            .and_then(|e| e.output_data.as_ref())
            .map(|d| d.interval.end)
            .unwrap_or(-1);
        let data_interval = OffsetInterval {
            start: prev_offset + 1,
            end: prev_offset + 10,
        };

        ds.commit_add_data(
            AddDataParams {
                input_checkpoint,
                output_data: Some(data_interval),
                output_watermark: None,
                source_state: None,
            },
            Some(OwnedFile::new(data_path)),
            Some(OwnedFile::new(checkpoint_path)),
            CommitOpts::default(),
        )
        .await
        .unwrap()
        .new_head
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
