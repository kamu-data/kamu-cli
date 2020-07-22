use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::*;

use chrono::Utc;
use std::convert::TryFrom;
use std::path::PathBuf;

pub struct MetadataRepositoryImpl {
    workspace_layout: WorkspaceLayout,
}

impl MetadataRepositoryImpl {
    pub fn new(workspace_layout: WorkspaceLayout) -> Self {
        Self {
            workspace_layout: workspace_layout,
        }
    }

    fn dataset_exists(&self, id: &DatasetID) -> bool {
        let path = self.get_dataset_metadata_dir(id);
        path.exists()
    }

    fn get_dataset_metadata_dir(&self, id: &DatasetID) -> PathBuf {
        self.workspace_layout.datasets_dir.join(id)
    }

    fn sort_snapshots_in_dependency_order(&self, snapshots: &mut Vec<DatasetSnapshot>) {
        if snapshots.len() > 1 {
            unimplemented!();
        }
    }
}

impl MetadataRepository for MetadataRepositoryImpl {
    fn iter_datasets(&self) -> Box<dyn Iterator<Item = DatasetIDBuf>> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.datasets_dir).unwrap();
        Box::new(ListDatasetsIter { rd: read_dir })
    }

    fn add_dataset(&mut self, snapshot: DatasetSnapshot) -> Result<(), DomainError> {
        let dataset_metadata_dir = self.get_dataset_metadata_dir(&snapshot.id);

        if dataset_metadata_dir.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Dataset,
                String::from(&snapshot.id as &str),
            ));
        }

        match snapshot.source {
            DatasetSource::Derivative { ref inputs, .. } => {
                for input_id in inputs {
                    if !self.dataset_exists(input_id) {
                        return Err(DomainError::missing_reference(
                            ResourceKind::Dataset,
                            String::from(&snapshot.id as &str),
                            ResourceKind::Dataset,
                            String::from(input_id as &str),
                        ));
                    }
                }
            }
            _ => (),
        }

        let first_block = MetadataBlock {
            block_hash: "".to_owned(),
            prev_block_hash: "".to_owned(),
            system_time: Utc::now(),
            source: Some(snapshot.source),
            output_slice: None,
            output_watermark: None,
            input_slices: None,
        };

        MetadataChainImpl::create(dataset_metadata_dir, first_block).map_err(|e| e.into())?;
        Ok(())
    }

    fn add_datasets(
        &mut self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<(), DomainError>)> {
        let mut snapshots_ordered = snapshots;
        self.sort_snapshots_in_dependency_order(&mut snapshots_ordered);

        snapshots_ordered
            .into_iter()
            .map(|s| {
                let id = s.id.clone();
                let res = self.add_dataset(s);
                (id, res)
            })
            .collect()
    }

    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError> {
        let path = self.workspace_layout.datasets_dir.join(dataset_id.as_str());
        if !path.exists() {
            Err(DomainError::does_not_exist(
                ResourceKind::Dataset,
                (dataset_id as &str).to_owned(),
            ))
        } else {
            Ok(Box::new(MetadataChainImpl::new(path)))
        }
    }
}

struct ListDatasetsIter {
    rd: std::fs::ReadDir,
}
impl Iterator for ListDatasetsIter {
    type Item = DatasetIDBuf;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.rd.next()?;
        let path = res.unwrap();
        let name = path.file_name();
        Some(DatasetIDBuf::try_from(&name).unwrap())
    }
}
