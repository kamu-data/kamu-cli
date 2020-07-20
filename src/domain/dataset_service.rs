use super::DomainError;
use crate::domain::DatasetIDBuf;
use crate::infra::serde::yaml::DatasetSnapshot;

pub trait DatasetService {
    fn load_snapshot_from_ref(&self, snapshot_ref: &str) -> Result<DatasetSnapshot, DomainError>;

    fn create_datasets_from_snapshots(
        &mut self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<(), DomainError>)>;
}
