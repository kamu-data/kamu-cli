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

    /*async fn read_repo_info(&self) -> Result<DatasetRepositoryInfo, InternalError> {
        use crate::domain::repos::named_object_repository::GetError;

        let data = match self.info_repo.get("info").await {
            Ok(data) => data,
            Err(GetError::NotFound(_)) => {
                return Ok(DatasetRepositoryInfo {
                    datasets: Vec::new(),
                })
            }
            Err(GetError::Internal(e)) => return Err(e),
        };

        let manifest: Manifest<DatasetRepositoryInfo> =
            serde_yaml::from_slice(&data[..]).int_err()?;

        if manifest.kind != "DatasetRepositoryInfo" {
            return Err(InvalidObjectKind {
                expected: "DatasetRepositoryInfo".to_owned(),
                actual: manifest.kind,
            }
            .int_err()
            .into());
        }

        Ok(manifest.content)
    }

    async fn write_repo_info(&self, info: DatasetRepositoryInfo) -> Result<(), InternalError> {
        let manifest = Manifest {
            kind: "DatasetRepositoryInfo".to_owned(),
            version: 1,
            content: info,
        };

        let data = serde_yaml::to_vec(&manifest).int_err()?;

        self.info_repo.set("info", &data).await?;

        Ok(())
    }*/

    async fn finish_create_dataset(
        &self,
        dataset: &dyn Dataset,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, CreateDatasetError> {
        let summary = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => unreachable!(),
            Err(GetSummaryError::Access(e)) => Err(e.int_err().into()),
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, dataset_alias.clone());

        // // Add new entry
        // repo_info.datasets.push(DatasetEntry {
        //     id: handle.id.clone(),
        //     name: handle.name.clone(),
        // });

        // self.repo
        //     .write_repo_info(repo_info)
        //     .await
        //     .map_err(|e| CreateDatasetError::Internal(e.into()))?;

        Ok(handle)
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
                let hdl = self.resolve_dataset_ref(&name.into()).await.int_err()?;
                yield hdl;
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
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError> {
        let dataset_path = if dataset_alias.is_multitenant() {
            self.root
                .join(dataset_alias.account_name.as_ref().unwrap())
                .join(dataset_alias.dataset_name.as_str())
        } else {
            self.root.join(dataset_alias.dataset_name.as_str())
        };

        let folder_state = if dataset_path.exists() {
            DatasetBuilderFolderState::ExistingDataset
        } else {
            DatasetBuilderFolderState::UnfinishedNewDataset
        };

        let layout = DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = DatasetFactoryImpl::get_local_fs(layout);

        Ok(Box::new(DatasetBuilderLocalFs::new(
            self.clone(),
            dataset,
            dataset_path,
            dataset_alias.clone(),
            folder_state,
        )))
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

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetBuilderImpl
/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetBuilderLocalFs<D> {
    repo: DatasetRepositoryLocalFs,
    dataset: D,
    dataset_path: PathBuf,
    dataset_alias: DatasetAlias,
    folder_state: DatasetBuilderFolderState,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum DatasetBuilderFolderState {
    UnfinishedNewDataset,
    FinishedNewDataset,
    ExistingDataset,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D> DatasetBuilderLocalFs<D> {
    fn new(
        repo: DatasetRepositoryLocalFs,
        dataset: D,
        dataset_path: PathBuf,
        dataset_alias: DatasetAlias,
        folder_state: DatasetBuilderFolderState,
    ) -> Self {
        Self {
            repo,
            dataset,
            dataset_path,
            dataset_alias,
            folder_state,
        }
    }

    fn discard_impl(&self) -> Result<(), InternalError> {
        if self.folder_state == DatasetBuilderFolderState::UnfinishedNewDataset
            && self.dataset_path.exists()
        {
            std::fs::remove_dir_all(&self.dataset_path).int_err()?;
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D> Drop for DatasetBuilderLocalFs<D> {
    fn drop(&mut self) {
        let _ = self.discard_impl();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<D> DatasetBuilder for DatasetBuilderLocalFs<D>
where
    D: Dataset,
{
    fn as_dataset(&self) -> &dyn Dataset {
        &self.dataset
    }

    async fn finish(&mut self) -> Result<DatasetHandle, CreateDatasetError> {
        match self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
        {
            Ok(_) => Ok(()),
            Err(GetRefError::NotFound(_)) => {
                self.discard_impl()?;
                Err(CreateDatasetError::EmptyDataset)
            }
            Err(GetRefError::Access(e)) => Err(e.int_err().into()),
            Err(GetRefError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        self.repo
            .finish_create_dataset(&self.dataset, &self.dataset_alias)
            .await
            .map(|handle| {
                if self.folder_state == DatasetBuilderFolderState::UnfinishedNewDataset {
                    self.folder_state = DatasetBuilderFolderState::FinishedNewDataset;
                }
                handle
            })
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.discard_impl()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<D> Dataset for DatasetBuilderLocalFs<D>
where
    D: Dataset,
{
    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError> {
        self.dataset.get_summary(opts).await
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain {
        self.dataset.as_metadata_chain()
    }
    fn as_data_repo(&self) -> &dyn ObjectRepository {
        self.dataset.as_data_repo()
    }
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        self.dataset.as_checkpoint_repo()
    }
    fn as_cache_repo(&self) -> &dyn NamedObjectRepository {
        self.dataset.as_cache_repo()
    }
}
