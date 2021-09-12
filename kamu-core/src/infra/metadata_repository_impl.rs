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

    fn get_all_datasets_impl(&self) -> Result<impl Iterator<Item = DatasetIDBuf>, std::io::Error> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.datasets_dir)?;
        let it = read_dir.map(|i| DatasetIDBuf::try_from(&i.unwrap().file_name()).unwrap());
        Ok(it)
    }

    fn dataset_exists(&self, id: &DatasetID) -> bool {
        let path = self.get_dataset_metadata_dir(id);
        path.exists()
    }

    fn get_dataset_metadata_dir(&self, id: &DatasetID) -> PathBuf {
        self.workspace_layout.datasets_dir.join(id)
    }

    fn get_metadata_chain_impl(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<MetadataChainImpl, DomainError> {
        let path = self.get_dataset_metadata_dir(dataset_id);
        if !path.exists() {
            Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                (dataset_id as &str).to_owned(),
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
        let mut pending: HashSet<DatasetIDBuf> = snapshots.iter().map(|s| s.id.clone()).collect();
        let mut added: HashSet<DatasetIDBuf> = HashSet::new();

        // TODO: cycle detection
        while !snapshots.is_empty() {
            let head = snapshots.pop_front().unwrap();
            let has_deps = match head.source {
                DatasetSource::Derivative(ref src) => {
                    src.inputs.iter().any(|id| pending.contains(id))
                }
                _ => false,
            };
            if !has_deps {
                pending.remove(&head.id);
                added.insert(head.id.clone());
                ordered.push(head);
            } else {
                snapshots.push_back(head);
            }
        }
        ordered
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
            api_version: 1,
            kind: "DatasetSummary".to_owned(),
            content: summary,
        };
        let file = std::fs::File::create(&path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    // TODO: summaries should be per branch
    fn summarize(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError> {
        let chain = self.get_metadata_chain_impl(dataset_id)?;

        let mut summary = DatasetSummary {
            last_block_hash: chain.read_ref(&BlockRef::Head).unwrap(),
            ..DatasetSummary::new(dataset_id.to_owned())
        };

        for block in chain.iter_blocks() {
            match block.source {
                Some(DatasetSource::Root(_)) => {
                    summary.kind = DatasetKind::Root;
                }
                Some(DatasetSource::Derivative(src)) => {
                    summary.kind = DatasetKind::Derivative;
                    if summary.dependencies.is_empty() {
                        summary.dependencies = src.inputs;
                    }
                }
                None => (),
            }

            if let Some(slice) = block.output_slice {
                if summary.last_pulled.is_none() {
                    summary.last_pulled = Some(block.system_time);
                }
                summary.num_records += slice.num_records as u64;
            }
        }

        // Calculate data size
        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let layout = DatasetLayout::new(&volume_layout, dataset_id);
        summary.data_size = fs_extra::dir::get_size(layout.data_dir).unwrap_or(0);
        summary.data_size += fs_extra::dir::get_size(layout.checkpoints_dir).unwrap_or(0);

        Ok(summary)
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
            api_version: 1,
            kind: "DatasetConfig".to_owned(),
            content: config,
        };
        let file = std::fs::File::create(&path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn get_config(&self, dataset_id: &DatasetID) -> Result<DatasetConfig, DomainError> {
        if !self.dataset_exists(dataset_id) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_id.as_str().to_owned(),
            ));
        }

        let path = self.get_dataset_metadata_dir(dataset_id).join("config");

        if path.exists() {
            self.read_config(&path)
        } else {
            Ok(DatasetConfig::default())
        }
    }

    fn set_config(&self, dataset_id: &DatasetID, config: DatasetConfig) -> Result<(), DomainError> {
        if !self.dataset_exists(dataset_id) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_id.as_str().to_owned(),
            ));
        }

        let path = self.get_dataset_metadata_dir(dataset_id).join("config");
        self.write_config(&path, config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

impl MetadataRepository for MetadataRepositoryImpl {
    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's> {
        Box::new(self.get_all_datasets_impl().unwrap())
    }

    fn add_dataset(&self, snapshot: DatasetSnapshot) -> Result<Sha3_256, DomainError> {
        let first_block = MetadataBlock {
            block_hash: Sha3_256::zero(),
            prev_block_hash: None,
            system_time: Utc::now(),
            source: Some(snapshot.source),
            vocab: snapshot.vocab,
            output_slice: None,
            output_watermark: None,
            input_slices: None,
        };

        self.add_dataset_from_block(&snapshot.id, first_block)
    }

    fn add_dataset_from_block(
        &self,
        dataset_id: &DatasetID,
        first_block: MetadataBlock,
    ) -> Result<Sha3_256, DomainError> {
        assert!(first_block.prev_block_hash.is_none());
        assert!(first_block.source.is_some());

        let dataset_metadata_dir = self.get_dataset_metadata_dir(dataset_id);

        if dataset_metadata_dir.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Dataset,
                dataset_id.to_string(),
            ));
        }

        if let Some(DatasetSource::Derivative(ref src)) = first_block.source {
            for input_id in src.inputs.iter() {
                if !self.dataset_exists(input_id) {
                    return Err(DomainError::missing_reference(
                        ResourceKind::Dataset,
                        dataset_id.to_string(),
                        ResourceKind::Dataset,
                        input_id.to_string(),
                    ));
                }
            }
        };

        let (_chain, block_hash) =
            MetadataChainImpl::create(&dataset_metadata_dir, first_block).map_err(|e| e.into())?;

        Ok(block_hash)
    }

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<Sha3_256, DomainError>)> {
        let snapshots_ordered = self.sort_snapshots_in_dependency_order(snapshots.collect());

        snapshots_ordered
            .into_iter()
            .map(|s| {
                let id = s.id.clone();
                let res = self.add_dataset(s);
                (id, res)
            })
            .collect()
    }

    fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DomainError> {
        if !self.dataset_exists(dataset_id) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_id.as_str().to_owned(),
            ));
        }

        // TODO: avoid copying
        let owned_id = dataset_id.to_owned();

        let dependents: Vec<_> = self
            .get_all_datasets_impl()
            .unwrap()
            .filter(|id| id != dataset_id)
            .map(|id| self.get_summary(&id).unwrap())
            .filter(|s| s.dependencies.contains(&owned_id))
            .map(|s| s.id)
            .collect();

        if dependents.len() > 0 {
            return Err(DomainError::dangling_reference(
                dependents
                    .into_iter()
                    .map(|id| (ResourceKind::Dataset, id.as_str().to_owned()))
                    .collect(),
                ResourceKind::Dataset,
                dataset_id.as_str().to_owned(),
            ));
        }

        // TODO: should be handled differently
        let metadata_dir = self.get_dataset_metadata_dir(dataset_id);
        let volume_layout = VolumeLayout::new(&self.workspace_layout.local_volume_dir);
        let layout = DatasetLayout::new(&volume_layout, dataset_id);

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
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError> {
        self.get_metadata_chain_impl(dataset_id)
            .map(|c| Box::new(c) as Box<dyn MetadataChain>)
    }

    fn get_summary(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError> {
        if !self.dataset_exists(dataset_id) {
            return Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                dataset_id.as_str().to_owned(),
            ));
        }

        let path = self.get_dataset_metadata_dir(dataset_id).join("summary");

        let summary = if path.exists() {
            self.read_summary(&path)?
        } else {
            DatasetSummary::new(dataset_id.to_owned())
        };

        let chain = self.get_metadata_chain_impl(dataset_id)?;
        let last_block_hash = chain.read_ref(&BlockRef::Head).unwrap();

        if last_block_hash != summary.last_block_hash {
            let summary = self.summarize(dataset_id)?;
            self.write_summary(&path, summary.clone())?;
            Ok(summary)
        } else {
            Ok(summary)
        }
    }

    fn get_remote_aliases(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn RemoteAliases>, DomainError> {
        Ok(Box::new(RemoteAliasesImpl::new(
            self.clone(),
            dataset_id.to_owned(),
            self.get_config(dataset_id)?,
        )))
    }

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryBuf> + 's> {
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

    fn get_repository(&self, repo_id: &RepositoryID) -> Result<Repository, DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_id);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_id.to_string(),
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

    fn add_repository(&self, repo_id: &RepositoryID, url: Url) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_id);

        if file_path.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Repository,
                repo_id.to_string(),
            ));
        }

        let manifest = Manifest {
            api_version: 1,
            kind: "Repository".to_owned(),
            content: Repository { url: url },
        };

        let file = std::fs::File::create(&file_path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn delete_repository(&self, repo_id: &RepositoryID) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_id);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_id.to_string(),
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
    dataset_id: DatasetIDBuf,
    config: DatasetConfig,
}

impl RemoteAliasesImpl {
    fn new(
        metadata_repo: MetadataRepositoryImpl,
        dataset_id: DatasetIDBuf,
        config: DatasetConfig,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_id,
            config,
        }
    }
}

impl RemoteAliases for RemoteAliasesImpl {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a DatasetRef> + 'a> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        Box::new(aliases.iter().map(|r| r.as_ref()))
    }

    fn contains(&self, remote_ref: &DatasetRef, kind: RemoteAliasKind) -> bool {
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
        remote_ref: DatasetRefBuf,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        if !aliases.contains(&remote_ref) {
            aliases.push(remote_ref);
            self.metadata_repo
                .set_config(&self.dataset_id, self.config.clone())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete(
        &mut self,
        remote_ref: &DatasetRef,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        if let Some(i) = aliases.iter().position(|r| *r == *remote_ref) {
            aliases.remove(i);
            self.metadata_repo
                .set_config(&self.dataset_id, self.config.clone())?;
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
                .set_config(&self.dataset_id, self.config.clone())?;
        }
        Ok(len)
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataRepositoryNull;

impl MetadataRepository for MetadataRepositoryNull {
    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's> {
        Box::new(std::iter::empty())
    }

    fn add_dataset(&self, _snapshot: DatasetSnapshot) -> Result<Sha3_256, DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<Sha3_256, DomainError>)> {
        snapshots
            .map(|s| (s.id, Err(DomainError::ReadOnly)))
            .collect()
    }

    fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_id.as_str().to_owned(),
        ))
    }

    fn add_dataset_from_block(
        &self,
        _dataset_id: &DatasetID,
        _first_block: MetadataBlock,
    ) -> Result<Sha3_256, DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_id.as_str().to_owned(),
        ))
    }

    fn get_summary(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_id.as_str().to_owned(),
        ))
    }

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryBuf> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(&self, repo_id: &RepositoryID) -> Result<Repository, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_id.to_string(),
        ))
    }

    fn add_repository(&self, _repo_id: &RepositoryID, _url: Url) -> Result<(), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn delete_repository(&self, repo_id: &RepositoryID) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_id.to_string(),
        ))
    }

    fn get_remote_aliases(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn RemoteAliases>, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Dataset,
            dataset_id.as_str().to_owned(),
        ))
    }
}
