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
use opendatafabric::*;

use chrono::Utc;
use dill::*;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct MetadataRepositoryImpl {
    workspace_layout: Arc<WorkspaceLayout>,
}

////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl MetadataRepositoryImpl {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }

    // TODO: PERF: Resolving handles currently involves reading summary files
    fn get_all_datasets_impl(
        &self,
    ) -> Result<impl Iterator<Item = DatasetHandle> + '_, std::io::Error> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.datasets_dir)?;
        let it = read_dir
            .map(|i| DatasetName::try_from(&i.unwrap().file_name()).unwrap())
            .map(|name| self.resolve_dataset_ref(&name.into()).unwrap());
        Ok(it)
    }

    fn dataset_exists(&self, name: &DatasetName) -> bool {
        let path = self.get_dataset_metadata_dir(name);
        path.exists()
    }

    fn get_dataset_metadata_dir(&self, name: &DatasetName) -> PathBuf {
        self.workspace_layout.datasets_dir.join(name)
    }

    fn get_metadata_chain_impl(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<MetadataChainImpl, DomainError> {
        let path = self.get_dataset_metadata_dir(dataset_name);
        if !path.exists() {
            Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_name.into(),
            ))
        } else {
            Ok(MetadataChainImpl::new(&path))
        }
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

    fn resolve_transform_inputs(
        &self,
        dataset_name: &DatasetName,
        inputs: &mut Vec<TransformInput>,
    ) -> Result<(), DomainError> {
        for input in inputs.iter_mut() {
            if let Some(input_id) = &input.id {
                // Input is referenced by ID - in this case we allow any name
                self.resolve_dataset_ref(&input_id.as_local_ref())
                    .map_err(|e| match e {
                        DomainError::DoesNotExist { .. } => DomainError::missing_reference(
                            ResourceKind::Dataset,
                            dataset_name.to_string(),
                            ResourceKind::Dataset,
                            input_id.to_string(),
                        ),
                        _ => e,
                    })?;
            } else {
                // When ID is not specified we try resolving it by name
                let hdl = self
                    .resolve_dataset_ref(&input.name.as_local_ref())
                    .map_err(|e| match e {
                        DomainError::DoesNotExist { .. } => DomainError::missing_reference(
                            ResourceKind::Dataset,
                            dataset_name.to_string(),
                            ResourceKind::Dataset,
                            input.name.to_string(),
                        ),
                        _ => e,
                    })?;

                input.id = Some(hdl.id);
            }
        }
        Ok(())
    }

    fn get_summary_impl(&self, dataset_name: &DatasetName) -> Result<DatasetSummary, DomainError> {
        let path = self.get_dataset_metadata_dir(&dataset_name).join("summary");

        if path.exists() {
            let summary = self.read_summary(&path)?;
            let chain = self.get_metadata_chain_impl(&dataset_name)?;
            let last_block_hash = chain.read_ref(&BlockRef::Head).unwrap();
            if last_block_hash == summary.last_block_hash {
                Ok(summary)
            } else {
                let summary = self.summarize(&dataset_name)?;
                self.write_summary(&path, summary.clone())?;
                Ok(summary)
            }
        } else {
            let summary = self.summarize(&dataset_name)?;
            self.write_summary(&path, summary.clone())?;
            Ok(summary)
        }
    }

    fn read_summary(&self, path: &Path) -> Result<DatasetSummary, DomainError> {
        let file = std::fs::File::open(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to open the summary file at {}: {}",
                path.display(),
                e
            )
        });

        let manifest: Manifest<DatasetSummary> =
            serde_yaml::from_reader(&file).unwrap_or_else(|e| {
                panic!(
                    "Failed to deserialize the DatasetSummary at {}: {}",
                    path.display(),
                    e
                )
            });

        assert_eq!(manifest.kind, "DatasetSummary");
        Ok(manifest.content)
    }

    fn write_summary(&self, path: &Path, summary: DatasetSummary) -> Result<(), DomainError> {
        let manifest = Manifest {
            kind: "DatasetSummary".to_owned(),
            version: 1,
            content: summary,
        };
        let file = std::fs::File::create(&path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    // TODO: summaries should be per branch
    fn summarize(&self, dataset_name: &DatasetName) -> Result<DatasetSummary, DomainError> {
        let chain = self.get_metadata_chain_impl(dataset_name)?;

        let last_block_hash = chain.read_ref(&BlockRef::Head).unwrap();

        let mut id = None;
        let mut kind = None;
        let mut dependencies = Vec::new();
        let mut last_pulled = None;
        let mut num_records = 0;

        for (_, block) in chain.iter_blocks() {
            match block.event {
                MetadataEvent::Seed(seed) => {
                    id = Some(seed.dataset_id);
                    kind = Some(seed.dataset_kind);
                }
                MetadataEvent::SetTransform(set_transform) => {
                    if dependencies.is_empty() {
                        dependencies = set_transform.inputs;
                    }
                }
                MetadataEvent::AddData(add_data) => {
                    if last_pulled.is_none() {
                        last_pulled = Some(block.system_time);
                    }
                    let iv = add_data.output_data.interval;
                    num_records += (iv.end - iv.start + 1) as u64;
                }
                MetadataEvent::ExecuteQuery(execute_query) => {
                    if last_pulled.is_none() {
                        last_pulled = Some(block.system_time);
                    }
                    if let Some(output_data) = execute_query.output_data {
                        num_records +=
                            (output_data.interval.end - output_data.interval.start + 1) as u64;
                    }
                }
                MetadataEvent::SetPollingSource(_)
                | MetadataEvent::SetWatermark(_)
                | MetadataEvent::SetVocab(_) => (),
            }
        }

        // Calculate data size
        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let layout = DatasetLayout::new(&volume_layout, dataset_name);
        let data_size = fs_extra::dir::get_size(layout.data_dir).unwrap_or(0)
            + fs_extra::dir::get_size(layout.checkpoints_dir).unwrap_or(0);

        Ok(DatasetSummary {
            id: id.unwrap(),
            name: dataset_name.to_owned(),
            kind: kind.unwrap(),
            dependencies,
            last_block_hash,
            last_pulled,
            num_records,
            data_size,
        })
    }

    fn read_config(&self, path: &Path) -> Result<DatasetConfig, DomainError> {
        let file = std::fs::File::open(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to open the config file at {}: {}",
                path.display(),
                e
            )
        });

        let manifest: Manifest<DatasetConfig> =
            serde_yaml::from_reader(&file).unwrap_or_else(|e| {
                panic!(
                    "Failed to deserialize the DatasetConfig at {}: {}",
                    path.display(),
                    e
                )
            });

        assert_eq!(manifest.kind, "DatasetConfig");
        Ok(manifest.content)
    }

    fn write_config(&self, path: &Path, config: DatasetConfig) -> Result<(), DomainError> {
        let manifest = Manifest {
            kind: "DatasetConfig".to_owned(),
            version: 1,
            content: config,
        };
        let file = std::fs::File::create(&path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn get_config(&self, dataset_name: &DatasetName) -> Result<DatasetConfig, DomainError> {
        if !self.dataset_exists(dataset_name) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_name.into(),
            ));
        }

        let path = self.get_dataset_metadata_dir(dataset_name).join("config");

        if path.exists() {
            self.read_config(&path)
        } else {
            Ok(DatasetConfig::default())
        }
    }

    fn set_config(
        &self,
        dataset_name: &DatasetName,
        config: DatasetConfig,
    ) -> Result<(), DomainError> {
        if !self.dataset_exists(dataset_name) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_name.into(),
            ));
        }

        let path = self.get_dataset_metadata_dir(dataset_name).join("config");
        self.write_config(&path, config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

impl MetadataRepository for MetadataRepositoryImpl {
    // TODO: Can workspace contain multiple datasets with same ID?
    // TODO: PERF: Resolving handles currently involves reading summary files
    fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, DomainError> {
        match dataset_ref {
            DatasetRefLocal::ID(id) => {
                let read_dir = std::fs::read_dir(&self.workspace_layout.datasets_dir)
                    .map_err(|e| DomainError::InfraError(e.into()))?;

                let summary = read_dir
                    .map(|i| DatasetName::try_from(&i.unwrap().file_name()).unwrap())
                    .map(|name| self.get_summary_impl(&name).unwrap())
                    .filter(|s| s.id == *id)
                    .next()
                    .ok_or_else(|| {
                        DomainError::does_not_exist(ResourceKind::Dataset, id.to_did_string())
                    })?;

                Ok(DatasetHandle::new(summary.id, summary.name))
            }
            DatasetRefLocal::Name(name) => {
                let summary = self.get_summary_impl(&name)?;
                Ok(DatasetHandle::new(summary.id, summary.name))
            }
            DatasetRefLocal::Handle(h) => Ok(h.clone()),
        }
    }

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetHandle> + 's> {
        Box::new(self.get_all_datasets_impl().unwrap())
    }

    fn add_dataset(
        &self,
        mut snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), DomainError> {
        // Validate / resolve events
        for event in snapshot.metadata.iter_mut() {
            match event {
                MetadataEvent::Seed(_) => Err(DomainError::bad_input(
                    "Seed event is generated and cannot be specified explicitly",
                )),
                MetadataEvent::SetPollingSource(_) => {
                    if snapshot.kind != DatasetKind::Root {
                        Err(DomainError::bad_input(
                            "SetPollingSource is only allowed on root datasets",
                        ))
                    } else {
                        Ok(())
                    }
                }
                MetadataEvent::SetTransform(e) => {
                    if snapshot.kind != DatasetKind::Derivative {
                        Err(DomainError::bad_input(
                            "SetTransform is only allowed on derivative datasets",
                        ))
                    } else {
                        self.resolve_transform_inputs(&snapshot.name, &mut e.inputs)?;
                        Ok(())
                    }
                }
                MetadataEvent::SetVocab(_) => Ok(()),
                MetadataEvent::AddData(_)
                | MetadataEvent::ExecuteQuery(_)
                | MetadataEvent::SetWatermark(_) => Err(DomainError::bad_input(format!(
                    "Event is not allowed to appear in a DatasetSnapshot: {:?}",
                    event
                ))),
            }?;
        }

        let system_time = Utc::now();
        let mut blocks = Vec::new();

        // We are generating a key pair and deriving a dataset ID from it.
        // The key pair is discarded for now, but in future can be used for
        // proof of control over dataset and metadata signing.
        let (_keypair, dataset_id) = DatasetID::from_new_keypair_ed25519();
        blocks.push(MetadataBlock {
            system_time,
            prev_block_hash: None,
            event: MetadataEvent::Seed(Seed {
                dataset_id,
                dataset_kind: snapshot.kind,
            }),
        });

        for event in snapshot.metadata {
            blocks.push(MetadataBlock {
                system_time,
                prev_block_hash: None,
                event,
            });
        }

        self.add_dataset_from_blocks(&snapshot.name, &mut blocks.into_iter())
    }

    fn add_dataset_from_blocks(
        &self,
        dataset_name: &DatasetName,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(DatasetHandle, Multihash), DomainError> {
        let first_block = blocks.next().expect("Empty block chain");
        let seed = first_block
            .event
            .as_variant::<Seed>()
            .expect("First block has to contain seed");

        let dataset_id = seed.dataset_id.clone();

        let dataset_metadata_dir = self.get_dataset_metadata_dir(dataset_name);
        if dataset_metadata_dir.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Dataset,
                dataset_name.into(),
            ));
        }

        let (mut chain, mut head) =
            MetadataChainImpl::create(&dataset_metadata_dir, first_block).map_err(|e| e.into())?;

        for mut block in blocks {
            if block.prev_block_hash.is_none() {
                block.prev_block_hash = Some(head);
            }
            head = chain.append(block);
        }

        Ok((
            DatasetHandle {
                id: dataset_id,
                name: dataset_name.clone(),
            },
            head,
        ))
    }

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetName, Result<(DatasetHandle, Multihash), DomainError>)> {
        let snapshots_ordered = self.sort_snapshots_in_dependency_order(snapshots.collect());

        snapshots_ordered
            .into_iter()
            .map(|s| {
                let name = s.name.clone();
                let res = self.add_dataset(s);
                (name, res)
            })
            .collect()
    }

    fn delete_dataset(&self, dataset_ref: &DatasetRefLocal) -> Result<(), DomainError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref)?;

        let dependents: Vec<_> = self
            .get_all_datasets_impl()
            .unwrap()
            .filter(|hdl| hdl.id != dataset_handle.id)
            .map(|hdl| self.get_summary(&hdl.into()).unwrap())
            .filter(|s| {
                s.dependencies
                    .iter()
                    .filter(|d| d.id == Some(dataset_handle.id.clone()))
                    .next()
                    .is_some()
            })
            .map(|s| s.name)
            .collect();

        if dependents.len() > 0 {
            return Err(DomainError::dangling_reference(
                dependents
                    .into_iter()
                    .map(|name| (ResourceKind::Dataset, name.into()))
                    .collect(),
                ResourceKind::Dataset,
                dataset_handle.name.to_string(),
            ));
        }

        // TODO: should be handled differently
        let metadata_dir = self.get_dataset_metadata_dir(&dataset_handle.name);
        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let layout = DatasetLayout::new(&volume_layout, &dataset_handle.name);

        let paths = [
            layout.cache_dir,
            layout.checkpoints_dir,
            layout.data_dir,
            metadata_dir,
        ];

        for p in paths.iter() {
            if p.exists() {
                std::fs::remove_dir_all(p).unwrap_or_else(|e| {
                    panic!("Failed to remove directory {}: {}", p.display(), e)
                });
            }
        }

        Ok(())
    }

    fn get_metadata_chain(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn MetadataChain>, DomainError> {
        let hdl = self.resolve_dataset_ref(dataset_ref)?;
        self.get_metadata_chain_impl(&hdl.name)
            .map(|c| Box::new(c) as Box<dyn MetadataChain>)
    }

    fn get_summary(&self, dataset_ref: &DatasetRefLocal) -> Result<DatasetSummary, DomainError> {
        let dataset_name = match dataset_ref {
            DatasetRefLocal::ID(_) => self.resolve_dataset_ref(dataset_ref)?.name,
            DatasetRefLocal::Name(name) => name.clone(),
            DatasetRefLocal::Handle(hdl) => hdl.name.clone(),
        };

        self.get_summary_impl(&dataset_name)
    }

    fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn RemoteAliases>, DomainError> {
        let hdl = self.resolve_dataset_ref(dataset_ref)?;
        let config = self.get_config(&hdl.name)?;
        Ok(Box::new(RemoteAliasesImpl::new(self.clone(), hdl, config)))
    }

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.repos_dir).unwrap();
        Box::new(read_dir.map(|i| {
            i.unwrap()
                .file_name()
                .into_string()
                .unwrap()
                .try_into()
                .unwrap()
        }))
    }

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        let file = std::fs::File::open(&file_path).unwrap_or_else(|e| {
            panic!(
                "Failed to open the Repository file at {}: {}",
                file_path.display(),
                e
            )
        });

        let manifest: Manifest<Repository> = serde_yaml::from_reader(&file).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize the Repository at {}: {}",
                file_path.display(),
                e
            )
        });

        assert_eq!(manifest.kind, "Repository");
        Ok(manifest.content)
    }

    fn add_repository(&self, repo_name: &RepositoryName, url: Url) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if file_path.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        let manifest = Manifest {
            kind: "Repository".to_owned(),
            version: 1,
            content: Repository { url: url },
        };

        let file = std::fs::File::create(&file_path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        std::fs::remove_file(&file_path).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasesImpl
////////////////////////////////////////////////////////////////////////////////////////

struct RemoteAliasesImpl {
    metadata_repo: MetadataRepositoryImpl,
    dataset_handle: DatasetHandle,
    config: DatasetConfig,
}

impl RemoteAliasesImpl {
    fn new(
        metadata_repo: MetadataRepositoryImpl,
        dataset_handle: DatasetHandle,
        config: DatasetConfig,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_handle,
            config,
        }
    }
}

impl RemoteAliases for RemoteAliasesImpl {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a RemoteDatasetName> + 'a> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        Box::new(aliases.iter())
    }

    fn contains(&self, remote_ref: &RemoteDatasetName, kind: RemoteAliasKind) -> bool {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        for a in aliases {
            if *a == *remote_ref {
                return true;
            }
        }
        false
    }

    fn is_empty(&self, kind: RemoteAliasKind) -> bool {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        aliases.is_empty()
    }

    fn add(
        &mut self,
        remote_ref: &RemoteDatasetName,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        let remote_ref = remote_ref.to_owned();
        if !aliases.contains(&remote_ref) {
            aliases.push(remote_ref);
            self.metadata_repo
                .set_config(&self.dataset_handle.name, self.config.clone())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete(
        &mut self,
        remote_ref: &RemoteDatasetName,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        if let Some(i) = aliases.iter().position(|r| *r == *remote_ref) {
            aliases.remove(i);
            self.metadata_repo
                .set_config(&self.dataset_handle.name, self.config.clone())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, DomainError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };
        let len = aliases.len();
        if !aliases.is_empty() {
            aliases.clear();
            self.metadata_repo
                .set_config(&self.dataset_handle.name, self.config.clone())?;
        }
        Ok(len)
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataRepositoryNull;

impl MetadataRepository for MetadataRepositoryNull {
    // Datasets
    fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_ref.to_string(),
        ))
    }

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetHandle> + 's> {
        Box::new(std::iter::empty())
    }

    fn add_dataset(
        &self,
        _snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn add_dataset_from_blocks(
        &self,
        _dataset_name: &DatasetName,
        _blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(DatasetHandle, Multihash), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetName, Result<(DatasetHandle, Multihash), DomainError>)> {
        snapshots
            .map(|s| (s.name, Err(DomainError::ReadOnly)))
            .collect()
    }

    fn delete_dataset(&self, dataset_ref: &DatasetRefLocal) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_ref.to_string(),
        ))
    }

    // Metadata

    fn get_metadata_chain(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn MetadataChain>, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_ref.to_string(),
        ))
    }

    // Dataset Extras

    fn get_summary(&self, dataset_ref: &DatasetRefLocal) -> Result<DatasetSummary, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_ref.to_string(),
        ))
    }

    fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn RemoteAliases>, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_ref.to_string(),
        ))
    }

    // Repositories

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_name.to_string(),
        ))
    }

    fn add_repository(&self, _repo_name: &RepositoryName, _url: Url) -> Result<(), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_name.to_string(),
        ))
    }
}
