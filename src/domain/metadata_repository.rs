use super::*;

pub trait MetadataRepository {
    fn list_datasets(&self) -> Vec<DatasetIDBuf>;
    fn get_metadata_chain(&self, dataset_id: &DatasetID) -> Box<dyn MetadataChain>;
}
