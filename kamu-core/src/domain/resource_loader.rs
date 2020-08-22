use super::DomainError;
use crate::infra::serde::yaml::DatasetSnapshot;

pub trait ResourceLoader {
    fn load_dataset_snapshot_from_path(
        &self,
        path: &std::path::Path,
    ) -> Result<DatasetSnapshot, DomainError>;

    fn load_dataset_snapshot_from_ref(&self, sref: &str) -> Result<DatasetSnapshot, DomainError>;
}
