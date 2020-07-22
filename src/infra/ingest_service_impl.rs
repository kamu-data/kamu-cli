use crate::domain::*;

pub struct IngestServiceImpl {
    //metadata_repo: &'a mut dyn MetadataRepository,
}

impl IngestServiceImpl {
    pub fn new(/*metadata_repo: &mut dyn MetadataRepository*/) -> IngestServiceImpl {
        IngestServiceImpl {
            //metadata_repo: metadata_repo,
        }
    }
}

impl IngestService for IngestServiceImpl {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        listener: Option<&mut dyn IngestListener>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }
}
