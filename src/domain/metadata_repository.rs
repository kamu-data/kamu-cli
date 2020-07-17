use super::*;

pub trait MetadataRepository {
    fn list_datasets(&self) -> Box<dyn Iterator<Item = DatasetIDBuf>>;
    fn get_metadata_chain(&self, dataset_id: &DatasetID) -> Result<Box<dyn MetadataChain>, Error>;
}
