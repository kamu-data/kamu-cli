// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::dataset_repository_helpers::get_staging_name;
use crate::domain::*;
use crate::infra::*;
use futures::TryStreamExt;
use opendatafabric::*;

use async_trait::async_trait;
use dill::*;
use std::path::{Path, PathBuf};
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
    fn get_dataset_impl(&self, dataset_name: &DatasetName) -> Result<impl Dataset, InternalError> {
        let layout = DatasetLayout::new(self.root.join(&dataset_name));
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
        staging_path: &Path,
        dataset_name: &DatasetName,
    ) -> Result<DatasetHandle, CreateDatasetError> {
        let summary = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => unreachable!(),
            Err(GetSummaryError::Access(e)) => Err(e.int_err().into()),
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, dataset_name.clone());

        // Check for late name collision
        if let Some(existing_dataset) = self
            .try_resolve_dataset_ref(&dataset_name.as_local_ref())
            .await?
        {
            return Err(NameCollisionError {
                name: existing_dataset.name,
            }
            .into());
        }

        // Atomic move
        let target_path = self.root.join(dataset_name);
        assert!(
            !target_path.exists(),
            "Target dir exists: {:?}",
            target_path
        );
        std::fs::rename(staging_path, target_path).int_err()?;

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
    async fn get_dataset_url(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Url, GetDatasetUrlError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        Ok(Url::from_directory_path(self.root.join(handle.name)).unwrap())
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
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRefLocal::Handle(h) => Ok(h.clone()),
            DatasetRefLocal::Name(name) => {
                let path = self.root.join(&name);
                if !path.exists() {
                    return Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }));
                }

                let dataset = self.get_dataset_impl(name)?;
                let summary = dataset
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .int_err()?;

                Ok(DatasetHandle::new(summary.id, name.clone()))
            }
            DatasetRefLocal::ID(id) => {
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

                    let name = DatasetName::try_from(&entry.file_name()).int_err()?;
                    let summary = self
                        .get_dataset_impl(&name)
                        .int_err()?
                        .get_summary(GetSummaryOpts::default())
                        .await
                        .int_err()?;
                    if summary.id == *id {
                        return Ok(DatasetHandle::new(summary.id, name));
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
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&handle.name)?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError> {
        let staging_path = self.root.join(get_staging_name());

        let layout = DatasetLayout::create(&staging_path).int_err()?;
        let dataset = DatasetFactoryImpl::get_local_fs(layout);

        Ok(Box::new(DatasetBuilderLocalFs::new(
            self.clone(),
            dataset,
            staging_path,
            dataset_name.clone(),
        )))
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        let old_name = self.resolve_dataset_ref(dataset_ref).await?.name;

        let old_dataset_path = self.root.join(&old_name);
        let new_dataset_path = self.root.join(&new_name);

        if new_dataset_path.exists() {
            Err(NameCollisionError {
                name: new_name.clone(),
            }
            .into())
        } else {
            // Atomic move
            std::fs::rename(old_dataset_path, new_dataset_path).int_err()?;
            Ok(())
        }
    }

    // TODO: PERF: Need fast inverse dependency lookup
    async fn delete_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<(), DeleteDatasetError> {
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

        let dataset_dir = self.root.join(&dataset_handle.name);
        tokio::fs::remove_dir_all(dataset_dir).await.int_err()?;
        Ok(())
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRefLocal,
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
    staging_path: PathBuf,
    dataset_name: DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D> DatasetBuilderLocalFs<D> {
    fn new(
        repo: DatasetRepositoryLocalFs,
        dataset: D,
        staging_path: PathBuf,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repo,
            dataset,
            staging_path,
            dataset_name,
        }
    }

    fn discard_impl(&self) -> Result<(), InternalError> {
        if self.staging_path.exists() {
            std::fs::remove_dir_all(&self.staging_path).int_err()?;
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

    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError> {
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
            .finish_create_dataset(&self.dataset, &self.staging_path, &self.dataset_name)
            .await
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
