use super::*;
use crate::infra::DatasetSummary;
use opendatafabric::*;

use url::Url;

pub trait MetadataRepository: Send + Sync {
    // Datasets

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's>;

    fn add_dataset(&self, snapshot: DatasetSnapshot) -> Result<Sha3_256, DomainError>;

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<Sha3_256, DomainError>)>;

    fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DomainError>;

    // Metadata

    // TODO: Separate mutable and immutable paths
    // See: https://github.com/rust-lang/rfcs/issues/2035
    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;

    // Dataset Extras

    fn get_summary(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError>;

    fn get_remote_aliases(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn RemoteAliases>, DomainError>;

    // Remotes

    fn get_all_remotes<'s>(&'s self) -> Box<dyn Iterator<Item = RemoteIDBuf> + 's>;

    fn get_remote(&self, remote_id: &RemoteID) -> Result<Remote, DomainError>;

    fn add_remote(&self, remote_id: &RemoteID, url: Url) -> Result<(), DomainError>;

    fn delete_remote(&self, remote_id: &RemoteID) -> Result<(), DomainError>;
}

pub trait DatasetDependencyVisitor {
    fn enter(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain) -> bool;
    fn exit(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain);
}
