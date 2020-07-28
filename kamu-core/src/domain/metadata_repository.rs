use super::*;

use crate::infra::serde::yaml::*;

pub trait MetadataRepository {
    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's>;

    fn visit_dataset_dependencies(
        &self,
        dataset_id: &DatasetID,
        visitor: &mut dyn DatasetDependencyVisitor,
    ) -> Result<(), DomainError>;

    fn get_datasets_in_dependency_order(
        &self,
        starting_dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
    ) -> Vec<DatasetIDBuf>;

    fn add_dataset(&mut self, snapshot: DatasetSnapshot) -> Result<(), DomainError>;

    fn add_datasets(
        &mut self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<(), DomainError>)>;

    // TODO: Separate mutable and immutable paths
    // See: https://github.com/rust-lang/rfcs/issues/2035
    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;

    fn get_summary(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError>;

    fn update_summary(
        &self,
        dataset_id: &DatasetID,
        summary: DatasetSummary,
    ) -> Result<(), DomainError>;
}

pub trait DatasetDependencyVisitor {
    fn enter(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain) -> bool;
    fn exit(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain);
}
