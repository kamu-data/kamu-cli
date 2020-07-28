use super::InfraError;
use crate::domain::*;
use crate::infra::serde::yaml::*;

use std::path::Path;

pub struct ResourceLoaderImpl {}

impl ResourceLoaderImpl {
    pub fn new() -> Self {
        Self {}
    }
}

impl ResourceLoader for ResourceLoaderImpl {
    fn load_dataset_snapshot_from_path(&self, path: &Path) -> Result<DatasetSnapshot, DomainError> {
        let file = std::fs::File::open(path).map_err(|e| InfraError::from(e).into())?;
        let manifest: Manifest<DatasetSnapshot> =
            serde_yaml::from_reader(file).map_err(|e| InfraError::from(e).into())?;
        assert_eq!(manifest.kind, "DatasetSnapshot");
        Ok(manifest.content)
    }

    fn load_dataset_snapshot_from_ref(&self, sref: &str) -> Result<DatasetSnapshot, DomainError> {
        self.load_dataset_snapshot_from_path(Path::new(sref))
    }
}
