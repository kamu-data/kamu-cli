use super::InfraError;
use crate::domain::*;
use crate::infra::serde::yaml::*;

use std::path::Path;

pub struct DatasetServiceImpl<'a> {
    metadata_repo: &'a mut dyn MetadataRepository,
}

impl DatasetServiceImpl<'_> {
    pub fn new(metadata_repo: &mut dyn MetadataRepository) -> DatasetServiceImpl {
        DatasetServiceImpl {
            metadata_repo: metadata_repo,
        }
    }

    fn load_snapshot_from_path(&self, path: &Path) -> Result<DatasetSnapshot, InfraError> {
        let file = std::fs::File::open(path)?;
        let manifest: Manifest<DatasetSnapshot> =
            serde_yaml::from_reader(file).map_err(|e| InfraError::SerdeError(e.into()))?;
        assert_eq!(manifest.kind, "DatasetSnapshot");
        Ok(manifest.content)
    }

    fn sort_snapshots_in_dependency_order(&self, snapshots: &mut Vec<DatasetSnapshot>) {}
}

impl DatasetService for DatasetServiceImpl<'_> {
    fn load_snapshot_from_ref(&self, snapshot_ref: &str) -> Result<DatasetSnapshot, DomainError> {
        self.load_snapshot_from_path(Path::new(snapshot_ref))
            .map_err(|e| e.into())
    }

    fn create_datasets_from_snapshots(
        &mut self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<(), DomainError>)> {
        let mut snapshots_ordered = snapshots;
        self.sort_snapshots_in_dependency_order(&mut snapshots_ordered);

        snapshots_ordered
            .into_iter()
            .map(|s| {
                let id = s.id.clone();
                let res = self.metadata_repo.add_dataset(s);
                (id, res)
            })
            .collect()
    }
}
