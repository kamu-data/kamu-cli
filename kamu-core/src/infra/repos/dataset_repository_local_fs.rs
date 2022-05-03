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
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRepositoryLocalFS {
    workspace_layout: Arc<WorkspaceLayout>,
    info_repo: NamedObjectRepositoryLocalFS,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRepositoryLocalFS {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        let info_repo = NamedObjectRepositoryLocalFS::new(&workspace_layout.kamu_root_dir);
        Self {
            workspace_layout,
            info_repo,
        }
    }

    fn get_dataset_impl(&self, dataset_name: &DatasetName) -> Result<impl Dataset, InternalError> {
        let path = self.workspace_layout.datasets_dir.join(&dataset_name);

        let layout = DatasetLayout::new(
            &VolumeLayout::new(&self.workspace_layout.local_volume_dir),
            dataset_name,
        );

        Ok(DatasetRepoFactory::get_local_fs_legacy(path, layout).into_internal_error()?)
    }

    async fn read_repo_info(&self) -> Result<DatasetRepositoryInfo, InternalError> {
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
            serde_yaml::from_slice(&data[..]).into_internal_error()?;

        if manifest.kind != "DatasetRepositoryInfo" {
            return Err(InvalidObjectKind {
                expected: "DatasetRepositoryInfo".to_owned(),
                actual: manifest.kind,
            }
            .into_internal_error()
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

        let data = serde_yaml::to_vec(&manifest).into_internal_error()?;

        self.info_repo.set("info", &data).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryLocalFS {
    // TODO: PERF: Cache data and speed up lookups
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRefLocal::Handle(h) => Ok(h.clone()),
            DatasetRefLocal::Name(name) => {
                let path = self.workspace_layout.datasets_dir.join(&name);
                if !path.exists() {
                    return Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }));
                }

                let dataset = self.get_dataset_impl(name)?;
                let summary = dataset
                    .get_summary(SummaryOptions::default())
                    .await
                    .into_internal_error()?;

                Ok(DatasetHandle::new(summary.id, name.clone()))
            }
            DatasetRefLocal::ID(id) => {
                let repo_info = self
                    .read_repo_info()
                    .await
                    .map_err(|e| GetDatasetError::Internal(e))?;

                let entry = repo_info
                    .datasets
                    .into_iter()
                    .filter(|d| d.id == *id)
                    .next()
                    .ok_or_else(|| {
                        GetDatasetError::NotFound(DatasetNotFoundError {
                            dataset_ref: dataset_ref.clone(),
                        })
                    })?;

                Ok(DatasetHandle::new(entry.id, entry.name))
            }
        }
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
        let tmp_path = self.workspace_layout.datasets_dir.join(".pending");

        std::fs::create_dir(&tmp_path).into_internal_error()?;
        std::fs::create_dir(tmp_path.join("blocks")).into_internal_error()?;
        std::fs::create_dir(tmp_path.join("refs")).into_internal_error()?;

        let layout = DatasetLayout::create(
            &VolumeLayout::new(&self.workspace_layout.local_volume_dir),
            &DatasetName::new_unchecked(".pending"),
        )
        .into_internal_error()?;

        let dataset = DatasetRepoFactory::get_local_fs_legacy(tmp_path.clone(), layout)
            .into_internal_error()?;

        Ok(Box::new(DatasetBuilderImpl::new(
            Self::new(self.workspace_layout.clone()),
            dataset,
            tmp_path,
            dataset_name.clone(),
        )))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl LocalDatasetRepository for DatasetRepositoryLocalFS {}

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetBuilderImpl<D> {
    repo: DatasetRepositoryLocalFS,
    dataset: D,
    dataset_path: PathBuf,
    dataset_name: DatasetName,
}

impl<D> DatasetBuilderImpl<D> {
    fn new(
        repo: DatasetRepositoryLocalFS,
        dataset: D,
        dataset_path: PathBuf,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repo,
            dataset,
            dataset_path,
            dataset_name,
        }
    }

    fn discard_impl(&self) -> Result<(), InternalError> {
        if self.dataset_path.exists() {
            let tmp_layout = DatasetLayout::new(
                &VolumeLayout::new(&self.repo.workspace_layout.local_volume_dir),
                &DatasetName::new_unchecked(".pending"),
            );
            std::fs::remove_dir_all(&self.dataset_path).into_internal_error()?;
            std::fs::remove_dir_all(&tmp_layout.cache_dir).into_internal_error()?;
            std::fs::remove_dir_all(&tmp_layout.data_dir).into_internal_error()?;
            std::fs::remove_dir_all(&tmp_layout.checkpoints_dir).into_internal_error()?;
        }

        Ok(())
    }
}

impl<D> Drop for DatasetBuilderImpl<D> {
    fn drop(&mut self) {
        let _ = self.discard_impl();
    }
}

#[async_trait]
impl<D> DatasetBuilder for DatasetBuilderImpl<D>
where
    D: Dataset,
{
    fn as_dataset(&self) -> &dyn Dataset {
        &self.dataset
    }

    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError> {
        let tmp_layout = DatasetLayout::new(
            &VolumeLayout::new(&self.repo.workspace_layout.local_volume_dir),
            &DatasetName::new_unchecked(".pending"),
        );

        let summary = match self.dataset.get_summary(SummaryOptions::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => {
                self.discard_impl()?;
                Err(CreateDatasetError::EmptyDataset)
            }
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, self.dataset_name.clone());

        // Check for name collision
        let mut repo_info = self
            .repo
            .read_repo_info()
            .await
            .map_err(|e| CreateDatasetError::Internal(e))?;

        if let Some(existing) = repo_info.datasets.iter().find(|d| d.name == handle.name) {
            return Err(CreateDatasetError::Collision(CollisionError {
                new_dataset: handle,
                existing_dataset: DatasetHandle::new(existing.id.clone(), existing.name.clone()),
            }));
        }

        // TODO: Atomic move
        let dest_path = self.repo.workspace_layout.datasets_dir.join(&handle.name);
        let dest_layout = DatasetLayout::new(
            &VolumeLayout::new(&self.repo.workspace_layout.local_volume_dir),
            &self.dataset_name,
        );

        std::fs::rename(&self.dataset_path, dest_path).into_internal_error()?;

        std::fs::rename(tmp_layout.cache_dir, dest_layout.cache_dir).into_internal_error()?;

        std::fs::rename(tmp_layout.data_dir, dest_layout.data_dir).into_internal_error()?;

        std::fs::rename(tmp_layout.checkpoints_dir, dest_layout.checkpoints_dir)
            .into_internal_error()?;

        // Add new entry
        repo_info.datasets.push(DatasetEntry {
            id: handle.id.clone(),
            name: handle.name.clone(),
        });

        self.repo
            .write_repo_info(repo_info)
            .await
            .map_err(|e| CreateDatasetError::Internal(e.into()))?;

        Ok(handle)
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.discard_impl()?;
        Ok(())
    }
}

#[async_trait]
impl<D> Dataset for DatasetBuilderImpl<D>
where
    D: Dataset,
{
    async fn get_summary(&self, opts: SummaryOptions) -> Result<DatasetSummary, GetSummaryError> {
        self.dataset.get_summary(opts).await
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain2 {
        self.dataset.as_metadata_chain()
    }
    fn as_data_repo(&self) -> &dyn ObjectRepository {
        self.dataset.as_data_repo()
    }
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        self.dataset.as_checkpoint_repo()
    }
}
