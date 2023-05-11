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
use futures::TryStreamExt;
use opendatafabric::*;

use async_trait::async_trait;
use dill::*;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRepositoryLocalFs {
    root: PathBuf,
    //info_repo: NamedObjectRepositoryLocalFS,
    thrash_lock: tokio::sync::Mutex<()>,
}

// TODO: Find a better way to share state with dataset builder
impl Clone for DatasetRepositoryLocalFs {
    fn clone(&self) -> Self {
        Self::from(self.root.clone())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetRepositoryLocalFs {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self::from(&workspace_layout.datasets_dir)
    }

    pub fn from(root: impl Into<PathBuf>) -> Self {
        //let info_repo = NamedObjectRepositoryLocalFS::new(&workspace_layout.kamu_root_dir);
        Self {
            root: root.into(),
            //info_repo,
            thrash_lock: tokio::sync::Mutex::new(()),
        }
    }

    // TODO: Make dataset factory (and thus the hashing algo) configurable
    fn get_dataset_impl(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<impl Dataset, InternalError> {
        let layout = DatasetLayout::new(self.root.join(&dataset_alias.dataset_name));
        Ok(DatasetFactoryImpl::get_local_fs(layout))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRegistry for DatasetRepositoryLocalFs {
    async fn get_dataset_url(&self, dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        Ok(Url::from_directory_path(self.root.join(handle.alias.dataset_name)).unwrap())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryLocalFs {
    // TODO: PERF: Cache data and speed up lookups by ID
    //
    // TODO: CONCURRENCY: Since resolving ID to Name currently requires accessing all summaries
    // multiple threads calling this function or iterating all datasets can result in significant thrashing.
    // We use a lock here until we have a better solution.
    //
    // Note that this lock does not prevent concurrent updates to summaries, only reduces the chances of it.
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRef::Handle(h) => Ok(h.clone()),
            DatasetRef::Alias(alias) => {
                assert!(
                    !alias.is_multitenant(),
                    "Multitenancy is not supported by LocalFs repository"
                );

                let path = self.root.join(&alias.dataset_name);
                if !path.exists() {
                    return Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }));
                }

                let dataset = self.get_dataset_impl(&alias)?;
                let summary = dataset
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .map_err(|e| {
                        if let GetSummaryError::EmptyDataset = e {
                            GetDatasetError::NotFound(DatasetNotFoundError {
                                dataset_ref: dataset_ref.clone(),
                            })
                        } else {
                            GetDatasetError::Internal(e.int_err())
                        }
                    })?;

                Ok(DatasetHandle::new(summary.id, alias.clone()))
            }
            DatasetRef::ID(id) => {
                // Anti-thrashing lock (see comment above)
                let _lock_guard = self.thrash_lock.lock().await;

                let read_dir = std::fs::read_dir(&self.root).int_err()?;

                for r in read_dir {
                    let entry = r.int_err()?;
                    if let Some(s) = entry.file_name().to_str() {
                        if s.starts_with(".") {
                            continue;
                        }
                    }

                    let alias = DatasetAlias::new(
                        None,
                        DatasetName::try_from(&entry.file_name()).int_err()?,
                    );

                    let summary = self
                        .get_dataset_impl(&alias)
                        .int_err()?
                        .get_summary(GetSummaryOpts::default())
                        .await
                        .int_err()?;

                    if summary.id == *id {
                        return Ok(DatasetHandle::new(summary.id, alias));
                    }
                }

                Err(GetDatasetError::NotFound(DatasetNotFoundError {
                    dataset_ref: dataset_ref.clone(),
                }))
            }
        }
    }

    // TODO: PERF: Resolving handles currently involves reading summary files
    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let read_dir =
            std::fs::read_dir(&self.root).int_err()?;

            for r in read_dir {
                let entry = r.int_err()?;
                if let Some(s) = entry.file_name().to_str() {
                    if s.starts_with(".") {
                        continue;
                    }
                }
                let name = DatasetName::try_from(&entry.file_name()).int_err()?;
                match self.resolve_dataset_ref(&name.into()).await {
                    Ok(hdl) => { yield hdl; Ok(()) }
                    Err(GetDatasetError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e.int_err())
                }?;
            }
        })
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&handle.alias)?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        let dataset_id = seed_block.event.dataset_id.clone();
        let dataset_path = if dataset_alias.is_multitenant() {
            self.root
                .join(dataset_alias.account_name.as_ref().unwrap())
                .join(dataset_alias.dataset_name.as_str())
        } else {
            self.root.join(dataset_alias.dataset_name.as_str())
        };

        let layout = DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = DatasetFactoryImpl::get_local_fs(layout);

        // There are three possiblities at this point:
        // - Dataset did not exist before - continue normally
        // - Dataset was partially created before (no head yet) and was not GC'd - so we assume ownership
        // - Dataset existed before (has valid head) - we should error out with name collision
        let head = match dataset
            .as_metadata_chain()
            .append(
                seed_block.into(),
                AppendOpts {
                    // We are using head ref CAS to detect previous existence of a dataset
                    // as atomically as possible
                    check_ref_is: Some(None),
                    ..AppendOpts::default()
                },
            )
            .await
        {
            Ok(head) => Ok(head),
            Err(AppendError::RefCASFailed(_)) => {
                Err(CreateDatasetError::NameCollision(NameCollisionError {
                    alias: dataset_alias.clone(),
                }))
            }
            Err(err) => Err(err.int_err().into()),
        }?;

        Ok(CreateDatasetResult {
            dataset_handle: DatasetHandle::new(dataset_id, dataset_alias.clone()),
            dataset: Arc::new(dataset),
            head,
        })
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_alias: &DatasetAlias,
    ) -> Result<(), RenameDatasetError> {
        let old_alias = self.resolve_dataset_ref(dataset_ref).await?.alias;

        let old_dataset_path = self.root.join(&old_alias.dataset_name);
        let new_dataset_path = self.root.join(&new_alias.dataset_name);

        if new_dataset_path.exists() {
            Err(NameCollisionError {
                alias: new_alias.clone(),
            }
            .into())
        } else {
            // Atomic move
            std::fs::rename(old_dataset_path, new_dataset_path).int_err()?;
            Ok(())
        }
    }

    // TODO: PERF: Need fast inverse dependency lookup
    async fn delete_dataset(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => Ok(h),
            Err(GetDatasetError::NotFound(e)) => Err(DeleteDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => Err(DeleteDatasetError::Internal(e)),
        }?;

        let children: Vec<_> = get_downstream_dependencies_impl(self, dataset_ref)
            .try_collect()
            .await?;

        if !children.is_empty() {
            return Err(DanglingReferenceError {
                dataset_handle,
                children,
            }
            .into());
        }

        // // Update repo info
        // let mut repo_info = self.read_repo_info().await?;
        // let index = repo_info
        //     .datasets
        //     .iter()
        //     .position(|d| d.name == dataset_handle.name)
        //     .ok_or("Inconsistent repository info")
        //     .int_err()?;
        // repo_info.datasets.remove(index);
        // self.write_repo_info(repo_info).await?;

        let dataset_dir = self.root.join(&dataset_handle.alias.dataset_name);
        tokio::fs::remove_dir_all(dataset_dir).await.int_err()?;
        Ok(())
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRef,
    ) -> DatasetHandleStream<'s> {
        Box::pin(get_downstream_dependencies_impl(self, dataset_ref))
    }
}
