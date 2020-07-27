use super::*;

use crate::infra::serde::yaml::DatasetSnapshot;

pub trait MetadataRepository {
    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's>;

    fn visit_dataset_dependencies(
        &self,
        dataset_id: &DatasetID,
        visitor: &mut dyn DatasetDependencyVisitor,
    ) -> Result<(), DomainError>;

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

pub trait DatasetDependencyVisitor {
    fn enter(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain) -> bool;
    fn exit(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain);
}
