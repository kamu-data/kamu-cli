use super::*;
use crate::domain::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use dill::*;
use slog::Logger;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct SyncServiceImpl {
    workspace_layout: WorkspaceLayout,
    metadata_repo: Arc<dyn MetadataRepository>,
    remote_factory: Arc<RemoteFactory>,
    _logger: Logger,
}

#[component(pub)]
impl SyncServiceImpl {
    pub fn new(
        workspace_layout: &WorkspaceLayout,
        metadata_repo: Arc<dyn MetadataRepository>,
        remote_factory: Arc<RemoteFactory>,
        logger: Logger,
    ) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
            metadata_repo,
            remote_factory,
            _logger: logger,
        }
    }

    fn do_sync_from(
        &self,
        remote_dataset_ref: &DatasetRef,
        local_dataset_id: &DatasetID,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let remote_id = remote_dataset_ref.remote_id().unwrap_or_else(|| {
            panic!(
                "Non-remote reference passed to sync_from: {}",
                remote_dataset_ref
            )
        });

        let remote = self
            .metadata_repo
            .get_remote(remote_id)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RemoteDoesNotExist {
                    remote_id: remote_id.to_owned(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let client = self
            .remote_factory
            .get_remote_client(&remote)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let cl = client.lock().unwrap();

        let remote_head = match cl
            .read_ref(remote_dataset_ref)
            .map_err(|e| SyncError::ProtocolError(e.into()))?
        {
            Some(hash) => hash,
            None => {
                return Err(SyncError::RemoteDatasetDoesNotExist {
                    dataset_ref: remote_dataset_ref.to_owned(),
                })
            }
        };

        let chain = match self.metadata_repo.get_metadata_chain(local_dataset_id) {
            Ok(chain) => Some(chain),
            Err(DomainError::DoesNotExist { .. }) => None,
            Err(e @ _) => return Err(SyncError::InternalError(e.into())),
        };

        let local_head = chain.as_ref().and_then(|c| c.read_ref(&BlockRef::Head));

        if Some(remote_head) == local_head {
            return Ok(SyncResult::UpToDate);
        }

        let tmp_dir = self.workspace_layout.run_info_dir.join(local_dataset_id);
        std::fs::create_dir_all(&tmp_dir)?;

        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let dataset_layout = DatasetLayout::new(&volume_layout, local_dataset_id);

        let read_result = cl
            .read(remote_dataset_ref, remote_head, local_head, &tmp_dir)
            .map_err(|e| match e {
                RemoteError::DoesNotExist => SyncError::RemoteDatasetDoesNotExist {
                    dataset_ref: remote_dataset_ref.to_owned(),
                },
                e => e.into(),
            })?;

        let de = YamlMetadataBlockDeserializer;
        let new_blocks = read_result
            .blocks
            .iter()
            .map(|data| de.read_manifest(&data))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| match e {
                opendatafabric::serde::Error::InvalidHash { .. } => SyncError::Corrupted {
                    message: "Inconsistent metadata".to_owned(),
                    source: e.into(),
                },
                _ => SyncError::ProtocolError(e.into()),
            })?;

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

        // TODO: Remote assumption on block ordering
        match chain {
            None => {
                MetadataChainImpl::from_blocks(
                    &self.workspace_layout.datasets_dir.join(local_dataset_id),
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

    fn do_sync_to(
        &self,
        local_dataset_id: &DatasetID,
        remote_dataset_ref: &DatasetRef,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let chain = match self.metadata_repo.get_metadata_chain(local_dataset_id) {
            Ok(c) => c,
            Err(DomainError::DoesNotExist { .. }) => {
                return Err(SyncError::LocalDatasetDoesNotExist {
                    dataset_id: local_dataset_id.to_owned(),
                })
            }
            Err(e) => return Err(SyncError::InternalError(e.into())),
        };

        let remote_id = remote_dataset_ref.remote_id().unwrap_or_else(|| {
            panic!(
                "Non-remote reference passed to sync_to: {}",
                remote_dataset_ref
            )
        });

        let remote = self
            .metadata_repo
            .get_remote(remote_id)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RemoteDoesNotExist {
                    remote_id: remote_id.to_owned(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let client = self
            .remote_factory
            .get_remote_client(&remote)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let mut cl = client.lock().unwrap();

        let remote_head = cl
            .read_ref(remote_dataset_ref)
            .map_err(|e| SyncError::InternalError(e.into()))?;

        let local_head = chain.read_ref(&BlockRef::Head).unwrap();

        if remote_head == Some(local_head) {
            return Ok(SyncResult::UpToDate);
        }

        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let dataset_layout = DatasetLayout::new(&volume_layout, local_dataset_id);
        let metadata_dir = self.workspace_layout.datasets_dir.join(local_dataset_id);
        let blocks_dir = metadata_dir.join("blocks");

        let mut found_remote_head = false;

        let blocks_to_sync: Vec<(Sha3_256, Vec<u8>)> = chain
            .iter_blocks()
            .map(|b| b.block_hash)
            .take_while(|h| {
                if Some(*h) == remote_head {
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

        cl.write(
            remote_dataset_ref,
            remote_head,
            local_head,
            &mut blocks_to_sync.into_iter(),
            &mut data_files_to_sync.iter().map(|e| e as &Path),
            &dataset_layout.checkpoints_dir,
        )?;

        Ok(SyncResult::Updated {
            old_head: remote_head,
            new_head: local_head,
            num_blocks,
        })
    }
}

impl SyncService for SyncServiceImpl {
    fn sync_from(
        &self,
        remote_ref: &DatasetRef,
        local_id: &DatasetID,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError> {
        let lst = listener.unwrap_or(Arc::new(Mutex::new(NullSyncListener)));
        {
            lst.lock().unwrap().begin();
        }
        match self.do_sync_from(remote_ref, local_id, options) {
            Ok(result) => {
                lst.lock().unwrap().success(&result);
                Ok(result)
            }
            Err(err) => {
                lst.lock().unwrap().error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetRef, &DatasetID)>,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetRefBuf, DatasetIDBuf), Result<SyncResult, SyncError>)> {
        let mut results = Vec::new();

        for (remote_dataset_ref, local_dataset_id) in datasets {
            let lst = if let Some(ref l) = listener {
                l.lock()
                    .unwrap()
                    .begin_sync(local_dataset_id, remote_dataset_ref)
            } else {
                None
            };

            let res = self.sync_from(remote_dataset_ref, local_dataset_id, options.clone(), lst);
            results.push((
                (remote_dataset_ref.to_owned(), local_dataset_id.to_owned()),
                res,
            ));
        }

        results
    }

    fn sync_to(
        &self,
        local_id: &DatasetID,
        remote_ref: &DatasetRef,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError> {
        let lst = listener.unwrap_or(Arc::new(Mutex::new(NullSyncListener)));
        {
            lst.lock().unwrap().begin();
        }
        match self.do_sync_to(local_id, remote_ref, options) {
            Ok(result) => {
                lst.lock().unwrap().success(&result);
                Ok(result)
            }
            Err(err) => {
                lst.lock().unwrap().error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetID, &DatasetRef)>,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetIDBuf, DatasetRefBuf), Result<SyncResult, SyncError>)> {
        let mut results = Vec::new();

        for (local_dataset_id, remote_dataset_ref) in datasets {
            let lst = if let Some(ref l) = listener {
                l.lock()
                    .unwrap()
                    .begin_sync(local_dataset_id, remote_dataset_ref)
            } else {
                None
            };

            let res = self.sync_to(local_dataset_id, remote_dataset_ref, options.clone(), lst);
            results.push((
                (local_dataset_id.to_owned(), remote_dataset_ref.to_owned()),
                res,
            ));
        }

        results
    }
}
