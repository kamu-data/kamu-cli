// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use dill::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct SyncServiceImpl {
    workspace_layout: Arc<WorkspaceLayout>,
    dataset_reg: Arc<dyn DatasetRegistry>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    repository_factory: Arc<RepositoryFactory>,
}

#[component(pub)]
impl SyncServiceImpl {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        dataset_reg: Arc<dyn DatasetRegistry>,
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        repository_factory: Arc<RepositoryFactory>,
    ) -> Self {
        Self {
            workspace_layout,
            dataset_reg,
            remote_repo_reg,
            repository_factory,
        }
    }

    async fn do_sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        // TODO: REMOTE ID
        let remote_name = match remote_ref {
            DatasetRefRemote::ID(_) => {
                unimplemented!("Syncing remote dataset by ID is not yet supported")
            }
            DatasetRefRemote::RemoteName(name) => name,
            DatasetRefRemote::RemoteHandle(hdl) => &hdl.name,
            DatasetRefRemote::Url(_) => {
                unimplemented!("Syncing remote dataset by Url is not yet supported")
            }
        };

        let repo_name = remote_name.repository();
        let dataset_name = remote_name.as_name_with_owner();

        let repo = self
            .remote_repo_reg
            .get_repository(&repo_name)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: repo_name.clone(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let client = self
            .repository_factory
            .get_repository_client(&repo)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let remote_head = match client
            .read_ref(&dataset_name)
            .await
            .map_err(|e| SyncError::ProtocolError(e.into()))?
        {
            Some(hash) => hash,
            None => {
                return Err(SyncError::RemoteDatasetDoesNotExist {
                    dataset_ref: remote_ref.clone(),
                })
            }
        };

        let chain = match self
            .dataset_reg
            .get_metadata_chain(&local_name.as_local_ref())
        {
            Ok(chain) => Some(chain),
            Err(DomainError::DoesNotExist { .. }) => None,
            Err(e @ _) => return Err(SyncError::InternalError(e.into())),
        };

        let local_head = chain.as_ref().and_then(|c| c.read_ref(&BlockRef::Head));

        if Some(remote_head.clone()) == local_head {
            return Ok(SyncResult::UpToDate);
        }

        let tmp_dir = self.workspace_layout.run_info_dir.join(&local_name);
        std::fs::create_dir_all(&tmp_dir)?;

        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let dataset_layout = DatasetLayout::new(&volume_layout, &local_name);

        let read_result = client
            .read(&dataset_name, &remote_head, &local_head, &tmp_dir)
            .await
            .map_err(|e| match e {
                RepositoryError::DoesNotExist => SyncError::RemoteDatasetDoesNotExist {
                    dataset_ref: remote_ref.clone(),
                },
                e => e.into(),
            })?;

        // Validate block hashes
        for (remote_hash, block_data) in &read_result.blocks {
            let actual_hash = Multihash::from_digest_sha3_256(block_data);
            if actual_hash != *remote_hash {
                return Err(SyncError::Corrupted {
                    message: format!(
                        "Inconsistent metadata: Remote declared block hash {} but actual {}",
                        remote_hash, actual_hash
                    ),
                    source: None,
                });
            }
        }

        let de = FlatbuffersMetadataBlockDeserializer;
        let new_blocks = read_result
            .blocks
            .iter()
            .map(|(_, data)| de.read_manifest(&data))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SyncError::ProtocolError(e.into()))?;

        // TODO: Read only checkpoints and data for blocks we're syncing
        // TODO: This is very unsafe
        if dataset_layout.checkpoints_dir.exists() {
            std::fs::remove_dir_all(&dataset_layout.checkpoints_dir)?;
        }
        std::fs::create_dir_all(&dataset_layout.checkpoints_dir)?;
        if read_result.checkpoint_dir.exists() {
            fs_extra::dir::move_dir(
                &read_result.checkpoint_dir,
                &dataset_layout.checkpoints_dir,
                &fs_extra::dir::CopyOptions {
                    content_only: true,
                    ..fs_extra::dir::CopyOptions::default()
                },
            )?;
        }

        std::fs::create_dir_all(&dataset_layout.data_dir)?;
        for data_file in read_result.data_files.iter() {
            let new_data_file_path = dataset_layout
                .data_dir
                .join(data_file.file_name().expect("Data file without file name"));

            if !new_data_file_path.exists() {
                std::fs::rename(data_file, new_data_file_path)?;
            }
        }

        let num_blocks = new_blocks.len();

        // TODO: Remove assumption on block ordering
        match chain {
            None => {
                MetadataChainImpl::from_blocks(
                    &self.workspace_layout.datasets_dir.join(&local_name),
                    &mut new_blocks.iter().rev().map(|b| b.clone()),
                )
                .map_err(|e| SyncError::InternalError(e.into()))?;
                ()
            }
            Some(mut c) => {
                for block in new_blocks.iter().rev() {
                    c.append(block.clone());
                }
            }
        }

        // TODO: Error tolerance
        std::fs::remove_dir_all(&tmp_dir)?;

        // TODO: race condition on remote head
        Ok(SyncResult::Updated {
            old_head: local_head,
            new_head: remote_head,
            num_blocks,
        })
    }

    async fn do_sync_to(
        &self,
        local_ref: &DatasetRefLocal,
        remote_name: &RemoteDatasetName,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let local_handle =
            self.dataset_reg
                .resolve_dataset_ref(local_ref)
                .map_err(|e| match e {
                    DomainError::DoesNotExist { .. } => SyncError::LocalDatasetDoesNotExist {
                        dataset_ref: local_ref.clone(),
                    },
                    _ => SyncError::InternalError(e.into()),
                })?;

        let repo_name = remote_name.repository();
        let dataset_name = remote_name.as_name_with_owner();

        let repo = self
            .remote_repo_reg
            .get_repository(&repo_name)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: repo_name.clone(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let client = self
            .repository_factory
            .get_repository_client(&repo)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let remote_head = client.read_ref(&dataset_name).await?;

        let chain = self
            .dataset_reg
            .get_metadata_chain(&local_handle.as_local_ref())
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let local_head = chain.read_ref(&BlockRef::Head).unwrap();

        if remote_head == Some(local_head.clone()) {
            return Ok(SyncResult::UpToDate);
        }

        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let dataset_layout = DatasetLayout::new(&volume_layout, &local_handle.name);
        let metadata_dir = self.workspace_layout.datasets_dir.join(&local_handle.name);
        let blocks_dir = metadata_dir.join("blocks");

        let mut found_remote_head = false;

        let blocks_to_sync: Vec<(Multihash, Vec<u8>)> = chain
            .iter_blocks()
            .map(|(h, _)| h)
            .take_while(|h| {
                if Some(h.clone()) == remote_head {
                    found_remote_head = true;
                    false
                } else {
                    true
                }
            })
            .map(|h| {
                let block_path = blocks_dir.join(h.to_string());
                let data = std::fs::read(block_path)?;
                Ok((h, data))
            })
            .collect::<Result<_, std::io::Error>>()?;

        if !found_remote_head && remote_head.is_some() {
            return Err(SyncError::DatasetsDiverged {
                local_head: local_head,
                remote_head: remote_head.unwrap(),
            });
        }

        let data_files_to_sync: Vec<PathBuf> = if dataset_layout.data_dir.exists() {
            std::fs::read_dir(&dataset_layout.data_dir)?
                .map(|e| e.unwrap().path())
                .collect()
        } else {
            Vec::new()
        };

        let num_blocks = blocks_to_sync.len();

        client
            .write(
                &dataset_name,
                &remote_head,
                &local_head,
                &mut blocks_to_sync.into_iter(),
                &mut data_files_to_sync.iter().map(|e| e as &Path),
                &dataset_layout.checkpoints_dir,
            )
            .await?;

        Ok(SyncResult::Updated {
            old_head: remote_head,
            new_head: local_head,
            num_blocks,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl SyncService for SyncServiceImpl {
    async fn sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.do_sync_from(remote_ref, local_name, options).await {
            Ok(result) => {
                listener.success(&result);
                Ok(result)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    async fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefRemote, DatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefRemote, DatasetName),
        Result<SyncResult, SyncError>,
    )> {
        let mut results = Vec::new();

        for (remote_ref, local_name) in datasets {
            let res = self
                .sync_from(
                    &remote_ref,
                    &local_name,
                    options.clone(),
                    listener
                        .as_ref()
                        .and_then(|l| l.begin_sync(&local_name.as_local_ref(), &remote_ref)),
                )
                .await;

            results.push(((remote_ref, local_name), res));
        }

        results
    }

    async fn sync_to(
        &self,
        local_ref: &DatasetRefLocal,
        remote_name: &RemoteDatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.do_sync_to(local_ref, remote_name, options).await {
            Ok(result) => {
                listener.success(&result);
                Ok(result)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    async fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefLocal, RemoteDatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefLocal, RemoteDatasetName),
        Result<SyncResult, SyncError>,
    )> {
        let mut results = Vec::new();

        for (local_ref, remote_name) in datasets {
            let res = self
                .sync_to(
                    &local_ref,
                    &remote_name,
                    options.clone(),
                    listener
                        .as_ref()
                        .and_then(|l| l.begin_sync(&local_ref, &remote_name.as_remote_ref())),
                )
                .await;

            results.push(((local_ref, remote_name), res));
        }

        results
    }

    async fn delete(&self, remote_ref: &RemoteDatasetName) -> Result<(), SyncError> {
        let repo_name = remote_ref.repository();

        let repo = self
            .remote_repo_reg
            .get_repository(&repo_name)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: repo_name.clone(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let client = self
            .repository_factory
            .get_repository_client(&repo)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        client.delete(&remote_ref.as_name_with_owner()).await?;

        Ok(())
    }
}
