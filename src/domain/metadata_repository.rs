use super::*;

use crate::infra::serde::yaml::DatasetSnapshot;

pub trait MetadataRepository {
    fn list_datasets(&self) -> Box<dyn Iterator<Item = DatasetIDBuf>>;

    fn add_dataset(&mut self, snapshot: DatasetSnapshot) -> Result<(), DomainError>;

    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;
}
