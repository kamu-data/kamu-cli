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
use chrono::Utc;
use dill::*;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::path::PathBuf;
use std::sync::Arc;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalDatasetRepositoryImpl {
    workspace_layout: Arc<WorkspaceLayout>,
    info_repo: NamedObjectRepositoryLocalFS,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl LocalDatasetRepositoryImpl {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        let info_repo = NamedObjectRepositoryLocalFS::new(&workspace_layout.kamu_root_dir);
        Self {
            workspace_layout,
            info_repo,
        }
    }

    // TODO: Make dataset factory (and thus the hashing algo) configurable
    fn get_dataset_impl(&self, dataset_name: &DatasetName) -> Result<impl Dataset, InternalError> {
        let path = self.workspace_layout.datasets_dir.join(&dataset_name);

        let layout = DatasetLayout::new(
            &VolumeLayout::new(&self.workspace_layout.local_volume_dir),
            dataset_name,
        );

        Ok(DatasetFactoryImpl::get_local_fs_legacy(path, layout).into_internal_error()?)
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

    async fn resolve_transform_inputs(
        &self,
        dataset_name: &DatasetName,
        inputs: &mut Vec<TransformInput>,
    ) -> Result<(), CreateDatasetFromSnapshotError> {
        for input in inputs.iter_mut() {
            if let Some(input_id) = &input.id {
                // Input is referenced by ID - in this case we allow any name
                match self.resolve_dataset_ref(&input_id.as_local_ref()).await {
                    Ok(_) => Ok(()),
                    Err(GetDatasetError::NotFound(_)) => Err(
                        CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                            dataset_ref: dataset_name.into(),
                            missing_inputs: vec![input_id.as_local_ref()],
                        }),
                    ),
                    Err(GetDatasetError::Internal(e)) => Err(e.into()),
                }?;
            } else {
                // When ID is not specified we try resolving it by name
                let hdl = match self.resolve_dataset_ref(&input.name.as_local_ref()).await {
                    Ok(hdl) => Ok(hdl),
                    Err(GetDatasetError::NotFound(_)) => Err(
                        CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                            dataset_ref: dataset_name.into(),
                            missing_inputs: vec![input.name.as_local_ref()],
                        }),
                    ),
                    Err(GetDatasetError::Internal(e)) => Err(e.into()),
                }?;

                input.id = Some(hdl.id);
            }
        }
        Ok(())
    }

    fn sort_snapshots_in_dependency_order(
        &self,
        mut snapshots: LinkedList<DatasetSnapshot>,
    ) -> Vec<DatasetSnapshot> {
        let mut ordered = Vec::with_capacity(snapshots.len());
        let mut pending: HashSet<DatasetName> = snapshots.iter().map(|s| s.name.clone()).collect();
        let mut added: HashSet<DatasetName> = HashSet::new();

        // TODO: cycle detection
        while !snapshots.is_empty() {
            let snapshot = snapshots.pop_front().unwrap();

            let transform = snapshot
                .metadata
                .iter()
                .find_map(|e| e.as_variant::<SetTransform>());

            let has_pending_deps = if let Some(transform) = transform {
                transform
                    .inputs
                    .iter()
                    .any(|input| pending.contains(&input.name))
            } else {
                false
            };

            if !has_pending_deps {
                pending.remove(&snapshot.name);
                added.insert(snapshot.name.clone());
                ordered.push(snapshot);
            } else {
                snapshots.push_back(snapshot);
            }
        }
        ordered
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl LocalDatasetRepository for LocalDatasetRepositoryImpl {
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

    // TODO: PERF: Resolving handles currently involves reading summary files
    fn get_all_datasets<'s>(&'s self) -> DatasetListStream<'s> {
        Box::pin(async_stream::try_stream! {
            let read_dir =
            std::fs::read_dir(&self.workspace_layout.datasets_dir).into_internal_error()?;

            for r in read_dir {
                let entry = r.into_internal_error()?;
                let name = DatasetName::try_from(&entry.file_name()).into_internal_error()?;
                let hdl = self.resolve_dataset_ref(&name.into()).await.into_internal_error()?;
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
        let tmp_path = self.workspace_layout.datasets_dir.join(".pending");

        std::fs::create_dir(&tmp_path).into_internal_error()?;
        std::fs::create_dir(tmp_path.join("blocks")).into_internal_error()?;
        std::fs::create_dir(tmp_path.join("refs")).into_internal_error()?;

        let layout = DatasetLayout::create(
            &VolumeLayout::new(&self.workspace_layout.local_volume_dir),
            &DatasetName::new_unchecked(".pending"),
        )
        .into_internal_error()?;

        let dataset = DatasetFactoryImpl::get_local_fs_legacy(tmp_path.clone(), layout)
            .into_internal_error()?;

        Ok(Box::new(DatasetBuilderImpl::new(
            Self::new(self.workspace_layout.clone()),
            dataset,
            tmp_path,
            dataset_name.clone(),
        )))
    }

    async fn create_dataset_from_snapshot(
        &self,
        mut snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), CreateDatasetFromSnapshotError> {
        // Validate / resolve events
        for event in snapshot.metadata.iter_mut() {
            match event {
                MetadataEvent::Seed(_) => Err(InvalidSnapshotError {
                    reason: "Seed event is generated and cannot be specified explicitly".to_owned(),
                }
                .into()),
                MetadataEvent::SetPollingSource(_) => {
                    if snapshot.kind != DatasetKind::Root {
                        Err(InvalidSnapshotError {
                            reason: "SetPollingSource is only allowed on root datasets".to_owned(),
                        }
                        .into())
                    } else {
                        Ok(())
                    }
                }
                MetadataEvent::SetTransform(e) => {
                    if snapshot.kind != DatasetKind::Derivative {
                        Err(InvalidSnapshotError {
                            reason: "SetTransform is only allowed on derivative datasets"
                                .to_owned(),
                        }
                        .into())
                    } else {
                        self.resolve_transform_inputs(&snapshot.name, &mut e.inputs)
                            .await
                    }
                }
                MetadataEvent::SetAttachments(_)
                | MetadataEvent::SetInfo(_)
                | MetadataEvent::SetLicense(_)
                | MetadataEvent::SetVocab(_) => Ok(()),
                MetadataEvent::AddData(_)
                | MetadataEvent::ExecuteQuery(_)
                | MetadataEvent::SetWatermark(_) => Err(InvalidSnapshotError {
                    reason: format!(
                        "Event is not allowed to appear in a DatasetSnapshot: {:?}",
                        event
                    ),
                }
                .into()),
            }?;
        }

        let system_time = Utc::now();

        let builder = self.create_dataset(&snapshot.name).await?;
        let chain = builder.as_dataset().as_metadata_chain();

        // We are generating a key pair and deriving a dataset ID from it.
        // The key pair is discarded for now, but in future can be used for
        // proof of control over dataset and metadata signing.
        let (_keypair, dataset_id) = DatasetID::from_new_keypair_ed25519();

        let mut head = chain
            .append(
                MetadataBlock {
                    system_time,
                    prev_block_hash: None,
                    event: MetadataEvent::Seed(Seed {
                        dataset_id,
                        dataset_kind: snapshot.kind,
                    }),
                },
                AppendOpts::default(),
            )
            .await
            .into_internal_error()?;

        for event in snapshot.metadata {
            head = chain
                .append(
                    MetadataBlock {
                        system_time,
                        prev_block_hash: Some(head),
                        event,
                    },
                    AppendOpts::default(),
                )
                .await
                .into_internal_error()?;
        }

        let hdl = builder.finish().await?;
        Ok((hdl, head))
    }

    async fn create_datasets_from_snapshots(
        &self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<(DatasetHandle, Multihash), CreateDatasetFromSnapshotError>,
    )> {
        let snapshots_ordered =
            self.sort_snapshots_in_dependency_order(snapshots.into_iter().collect());

        use futures::StreamExt;
        futures::stream::iter(snapshots_ordered)
            .then(|s| async {
                let name = s.name.clone();
                let res = self.create_dataset_from_snapshot(s).await;
                (name, res)
            })
            .collect()
            .await
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

        let mut children = Vec::new();

        use futures::StreamExt;
        let mut dataset_handles = self.get_all_datasets();
        while let Some(res) = dataset_handles.next().await {
            let hdl = res?;
            if hdl.id == dataset_handle.id {
                continue;
            }

            let summary = self
                .get_dataset(&hdl.as_local_ref())
                .await
                .into_internal_error()?
                .get_summary(SummaryOptions::default())
                .await
                .into_internal_error()?;

            if summary
                .dependencies
                .iter()
                .any(|d| d.id.as_ref() == Some(&dataset_handle.id))
            {
                children.push(hdl)
            }
        }

        if !children.is_empty() {
            return Err(DanglingReferenceError {
                dataset_handle,
                children,
            }
            .into());
        }

        // Update repo info
        let mut repo_info = self.read_repo_info().await?;
        let index = repo_info
            .datasets
            .iter()
            .position(|d| d.name == dataset_handle.name)
            .ok_or(GenericError::new("Inconsistent repository info").into_internal_error())?;
        repo_info.datasets.remove(index);
        self.write_repo_info(repo_info).await?;

        let metadata_dir = self
            .workspace_layout
            .datasets_dir
            .join(&dataset_handle.name);
        let layout = DatasetLayout::new(
            &VolumeLayout::new(&self.workspace_layout.local_volume_dir),
            &dataset_handle.name,
        );

        let paths = [
            layout.cache_dir,
            layout.checkpoints_dir,
            layout.data_dir,
            metadata_dir,
        ];

        for p in paths.iter().filter(|p| p.exists()) {
            tokio::fs::remove_dir_all(p).await.into_internal_error()?;
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetBuilderImpl
/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetBuilderImpl<D> {
    repo: LocalDatasetRepositoryImpl,
    dataset: D,
    dataset_path: PathBuf,
    dataset_name: DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D> DatasetBuilderImpl<D> {
    fn new(
        repo: LocalDatasetRepositoryImpl,
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

/////////////////////////////////////////////////////////////////////////////////////////

impl<D> Drop for DatasetBuilderImpl<D> {
    fn drop(&mut self) {
        let _ = self.discard_impl();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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
