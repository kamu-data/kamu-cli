// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub struct RepositoryLocalFS {
    path: PathBuf,
}

impl RepositoryLocalFS {
    pub fn new(path: PathBuf) -> Self {
        Self { path: path }
    }
}

impl RepositoryClient for RepositoryLocalFS {
    fn read_ref(
        &self,
        dataset_ref: &RemoteDatasetName,
    ) -> Result<Option<Multihash>, RepositoryError> {
        let ref_path: PathBuf = [
            self.path.as_ref() as &OsStr,
            OsStr::new(dataset_ref.dataset().as_str()),
            OsStr::new("meta"),
            OsStr::new("refs"),
            OsStr::new("head"),
        ]
        .iter()
        .collect();
        if ref_path.exists() {
            let hash = std::fs::read_to_string(&ref_path)
                .map_err(|e| RepositoryError::protocol(e.into()))?;
            Ok(Some(
                Multihash::from_multibase_str(hash.trim()).expect("Malformed hash"),
            ))
        } else {
            Ok(None)
        }
    }

    // TODO: Locking
    fn write(
        &mut self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Option<Multihash>,
        new_head: &Multihash,
        blocks: &mut dyn Iterator<Item = (Multihash, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RepositoryError> {
        if self.read_ref(dataset_ref)? != *expected_head {
            return Err(RepositoryError::UpdatedConcurrently);
        }

        let out_dataset_dir = self.path.join(dataset_ref.dataset());
        let out_meta_dir = out_dataset_dir.join("meta");
        let out_blocks_dir = out_meta_dir.join("blocks");
        let out_refs_dir = out_meta_dir.join("refs");
        let out_checkpoint_dir = out_dataset_dir.join("checkpoint");
        let out_data_dir = out_dataset_dir.join("data");

        std::fs::create_dir_all(&out_blocks_dir)?;
        std::fs::create_dir_all(&out_refs_dir)?;
        std::fs::create_dir_all(&out_checkpoint_dir)?;
        std::fs::create_dir_all(&out_data_dir)?;

        for in_data_path in data_files {
            let out_data_path = out_data_dir.join(
                in_data_path
                    .file_name()
                    .expect("Data file without file_name"),
            );
            if !out_data_path.exists() {
                std::fs::copy(in_data_path, out_data_path)?;
            }
        }

        for (hash, data) in blocks {
            let block_path = out_blocks_dir.join(hash.to_string());
            std::fs::write(block_path, data)?;
        }

        // TODO: This is really bad but we need to
        // establish proper checkpoint naming and rotation first
        if out_checkpoint_dir.exists() {
            std::fs::remove_dir_all(&out_checkpoint_dir)?;
        }

        if checkpoint_dir.exists() {
            fs_extra::dir::copy(
                &checkpoint_dir,
                &out_checkpoint_dir,
                &fs_extra::dir::CopyOptions {
                    copy_inside: true,
                    ..fs_extra::dir::CopyOptions::default()
                },
            )
            .map_err(|e| match e.kind {
                fs_extra::error::ErrorKind::Io(io_error) => io_error.into(),
                _ => RepositoryError::protocol(e.into()),
            })?;
        }

        std::fs::write(out_refs_dir.join("head"), &new_head.to_string().as_bytes())?;
        Ok(())
    }

    fn read(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Multihash,
        last_seen_block: &Option<Multihash>,
        tmp_dir: &Path,
    ) -> Result<RepositoryReadResult, RepositoryError> {
        let in_dataset_dir = self.path.join(dataset_ref.dataset());
        if !in_dataset_dir.exists() {
            return Err(RepositoryError::DoesNotExist);
        }

        let in_meta_dir = in_dataset_dir.join("meta");
        let chain = MetadataChainImpl::new(&in_meta_dir);

        if chain.read_ref(&BlockRef::Head) != Some(expected_head.clone()) {
            return Err(RepositoryError::UpdatedConcurrently);
        }

        let mut result = RepositoryReadResult {
            blocks: Vec::new(),
            data_files: Vec::new(),
            checkpoint_dir: tmp_dir.join("checkpoint"),
        };

        let in_blocks_dir = in_meta_dir.join("blocks");
        let in_checkpoint_dir = in_dataset_dir.join("checkpoint");
        let in_data_dir = in_dataset_dir.join("data");

        let out_data_dir = tmp_dir.join("data");
        std::fs::create_dir_all(&result.checkpoint_dir)?;
        std::fs::create_dir_all(&out_data_dir)?;

        let mut found_last_seen_block = false;

        for (block_hash, _) in chain.iter_blocks() {
            if Some(block_hash.clone()) == *last_seen_block {
                found_last_seen_block = true;
                break;
            }
            let block_path = in_blocks_dir.join(block_hash.to_string());
            let data = std::fs::read(block_path)?;
            result.blocks.push((block_hash, data));
        }

        if !found_last_seen_block && last_seen_block.is_some() {
            return Err(RepositoryError::Diverged {
                remote_head: expected_head.clone(),
                local_head: last_seen_block.clone().unwrap(),
            });
        }

        // TODO: limit the set of files based on metadata
        if in_data_dir.exists() {
            for entry in std::fs::read_dir(&in_data_dir)? {
                let in_path = entry?.path();
                let out_path =
                    out_data_dir.join(in_path.file_name().expect("Data file without file name"));
                std::fs::copy(&in_path, &out_path)?;
                result.data_files.push(out_path);
            }
        }

        if in_checkpoint_dir.exists() {
            fs_extra::dir::copy(
                &in_checkpoint_dir,
                &result.checkpoint_dir,
                &fs_extra::dir::CopyOptions {
                    content_only: true,
                    copy_inside: true,
                    ..fs_extra::dir::CopyOptions::default()
                },
            )
            .map_err(|e| match e.kind {
                fs_extra::error::ErrorKind::Io(io_error) => io_error.into(),
                _ => RepositoryError::protocol(e.into()),
            })?;
        }

        Ok(result)
    }

    fn delete(&self, dataset_ref: &RemoteDatasetName) -> Result<(), RepositoryError> {
        let dataset_dir = self.path.join(dataset_ref.dataset());
        if !dataset_dir.exists() {
            return Err(RepositoryError::DoesNotExist);
        }

        std::fs::remove_dir_all(dataset_dir)?;

        Ok(())
    }

    fn search(&self, query: Option<&str>) -> Result<RepositorySearchResult, RepositoryError> {
        // TODO: Find a way to avoid this
        let repo_name = RepositoryName::try_from("undefined").unwrap();

        let query = query.unwrap_or_default();
        let mut datasets = Vec::new();

        for entry in std::fs::read_dir(&self.path)? {
            if let Some(file_name) = entry?.file_name().to_str() {
                if query.is_empty() || file_name.contains(query) {
                    let dataset_name = DatasetName::try_from(file_name).unwrap();
                    datasets.push(RemoteDatasetName::new(&repo_name, None, &dataset_name));
                }
            }
        }

        Ok(RepositorySearchResult { datasets })
    }
}
