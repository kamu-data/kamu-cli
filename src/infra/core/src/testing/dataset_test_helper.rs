// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use file_utils::OwnedFile;
use kamu_core::*;
use odf::dataset::DatasetLayout;

use super::ParquetWriterHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    #[allow(clippy::assigning_clones)]
    fn list_files(dir: &Path) -> Vec<PathBuf> {
        if !dir.exists() {
            return Vec::new();
        }

        let mut v = DatasetTestHelper::_list_files_rec(dir);

        for path in &mut v {
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
        dataset_registry: &dyn DatasetRegistry,
        dataset_ref: impl Into<odf::DatasetRef>,
        data_size: usize,
    ) -> odf::Multihash {
        let tmp_dir = tempfile::tempdir().unwrap();

        let resolved_dataset = dataset_registry
            .get_dataset_by_ref(&dataset_ref.into())
            .await
            .unwrap();

        use odf::dataset::{MetadataChainExt, TryStreamExtExt};

        let prev_data = resolved_dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| match b.event {
                odf::MetadataEvent::AddData(e) => Some(e),
                _ => None,
            })
            .try_first()
            .await
            .unwrap();

        let data_path = tmp_dir.path().join("data");
        let checkpoint_path = tmp_dir.path().join("checkpoint");
        ParquetWriterHelper::from_sample_data(&data_path).unwrap();

        FileTestHelper::create_random_file(&checkpoint_path, data_size);

        let prev_checkpoint = prev_data
            .as_ref()
            .and_then(|e| e.new_checkpoint.as_ref())
            .map(|c| c.physical_hash.clone());

        let prev_offset = prev_data
            .as_ref()
            .and_then(odf::metadata::AddData::last_offset);

        let num_records = 10;
        let start = prev_offset.map_or(0, |v| v + 1);
        let new_offset_interval = odf::metadata::OffsetInterval {
            start,
            end: start + num_records - 1,
        };

        resolved_dataset
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint,
                    prev_offset,
                    new_offset_interval: Some(new_offset_interval),
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(data_path)),
                Some(odf::dataset::CheckpointRef::New(OwnedFile::new(
                    checkpoint_path,
                ))),
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap()
            .new_head
    }
}

pub struct FileTestHelper {}

impl FileTestHelper {
    pub fn create_random_file(path: &Path, data_size: usize) -> usize {
        use rand::RngCore;

        let mut data = vec![0u8; data_size];
        rand::thread_rng().fill_bytes(&mut data);

        std::fs::write(path, data).unwrap();
        data_size
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
