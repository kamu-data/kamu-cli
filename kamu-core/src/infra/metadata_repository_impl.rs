use super::*;
use crate::domain::*;
use opendatafabric::*;

use chrono::Utc;
use dill::*;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataRepositoryImpl {
    workspace_layout: WorkspaceLayout,
}

////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl MetadataRepositoryImpl {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
        }
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

    fn write_summary(&self, path: &Path, summary: &DatasetSummary) -> Result<(), DomainError> {
        let manifest = Manifest {
            api_version: 1,
            kind: "DatasetSummary".to_owned(),
            content: summary.clone(),
        };
        let file = std::fs::File::create(&path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    // TODO: summaries should be per branch
    fn summarize(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError> {
        let chain = self.get_metadata_chain_impl(dataset_id)?;

        let mut summary = DatasetSummary {
            id: dataset_id.to_owned(),
            last_block_hash: chain.read_ref(&BlockRef::Head).unwrap(),
            ..DatasetSummary::default()
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
}

////////////////////////////////////////////////////////////////////////////////////////

impl MetadataRepository for MetadataRepositoryImpl {
    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's> {
        Box::new(self.get_all_datasets_impl().unwrap())
    }

    fn add_dataset(&self, snapshot: DatasetSnapshot) -> Result<Sha3_256, DomainError> {
        let dataset_metadata_dir = self.get_dataset_metadata_dir(&snapshot.id);

        if dataset_metadata_dir.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Dataset,
                String::from(&snapshot.id as &str),
            ));
        }

        if let DatasetSource::Derivative(ref src) = snapshot.source {
            for input_id in src.inputs.iter() {
                if !self.dataset_exists(input_id) {
                    return Err(DomainError::missing_reference(
                        ResourceKind::Dataset,
                        String::from(&snapshot.id as &str),
                        ResourceKind::Dataset,
                        String::from(input_id as &str),
                    ));
                }
            }
        };

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
            DatasetSummary::default()
        };

        let chain = self.get_metadata_chain_impl(dataset_id)?;
        let last_block_hash = chain.read_ref(&BlockRef::Head).unwrap();

        if last_block_hash != summary.last_block_hash {
            let summary = self.summarize(dataset_id)?;
            self.write_summary(&path, &summary)?;
            Ok(summary)
        } else {
            Ok(summary)
        }
    }

    fn get_all_remotes<'s>(&'s self) -> Box<dyn Iterator<Item = RemoteIDBuf> + 's> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.remotes_dir).unwrap();
        Box::new(read_dir.map(|i| i.unwrap().file_name().into_string().unwrap()))
    }

    fn get_remote(&self, remote_id: &RemoteID) -> Result<Remote, DomainError> {
        let file_path = self.workspace_layout.remotes_dir.join(remote_id);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Remote,
                remote_id.to_string(),
            ));
        }

        let file = std::fs::File::open(&file_path).unwrap_or_else(|e| {
            panic!(
                "Failed to open the Remote file at {}: {}",
                file_path.display(),
                e
            )
        });

        let manifest: Manifest<Remote> = serde_yaml::from_reader(&file).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize the Remote at {}: {}",
                file_path.display(),
                e
            )
        });

        assert_eq!(manifest.kind, "Remote");
        Ok(manifest.content)
    }

    fn add_remote(&self, remote_id: &RemoteID, url: Url) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.remotes_dir.join(remote_id);

        if file_path.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Remote,
                remote_id.to_string(),
            ));
        }

        let manifest = Manifest {
            api_version: 1,
            kind: "Remote".to_owned(),
            content: Remote { url: url },
        };

        let file = std::fs::File::create(&file_path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn delete_remote(&self, remote_id: &RemoteID) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.remotes_dir.join(remote_id);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Remote,
                remote_id.to_string(),
            ));
        }

        std::fs::remove_file(&file_path).map_err(|e| InfraError::from(e).into())?;
        Ok(())
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

    fn get_all_remotes<'s>(&'s self) -> Box<dyn Iterator<Item = RemoteIDBuf> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_remote(&self, remote_id: &RemoteID) -> Result<Remote, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Remote,
            remote_id.to_string(),
        ))
    }

    fn add_remote(&self, _remote_id: &RemoteID, _url: Url) -> Result<(), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn delete_remote(&self, remote_id: &RemoteID) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Remote,
            remote_id.to_string(),
        ))
    }
}
