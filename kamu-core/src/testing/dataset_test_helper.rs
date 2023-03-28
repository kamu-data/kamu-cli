// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use crate::infra::DatasetLayout;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetTestHelper {}

impl DatasetTestHelper {
    pub fn assert_datasets_in_sync(
        dataset_1_layout: &DatasetLayout,
        dataset_2_layout: &DatasetLayout,
    ) {
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
}

/////////////////////////////////////////////////////////////////////////////////////////
