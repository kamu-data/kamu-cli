use super::*;

use crate::infra::serde::yaml::DatasetSnapshot;

pub trait MetadataRepository {
    fn iter_datasets(&self) -> Box<dyn Iterator<Item = DatasetIDBuf>>;

    fn add_dataset(&mut self, snapshot: DatasetSnapshot) -> Result<(), DomainError>;

    fn add_datasets(
        &mut self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<(), DomainError>)>;

    // TODO: Separate mutable and immutable paths
    // See: https://github.com/rust-lang/rfcs/issues/2035
    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;
}
