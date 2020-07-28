use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

pub struct IngestTask<'a> {
    dataset_id: DatasetIDBuf,
    layout: DatasetLayout,
    source: DatasetSourceRoot,
    meta_chain: &'a mut dyn MetadataChain,
    listener: &'a mut dyn IngestListener,
}

impl IngestTask<'_> {
    pub fn new<'a>(
        dataset_id: &DatasetID,
        layout: DatasetLayout,
        meta_chain: &'a mut dyn MetadataChain,
        listener: &'a mut dyn IngestListener,
    ) -> IngestTask<'a> {
        // TODO: this is expensive
        let source = match meta_chain.iter_blocks().filter_map(|b| b.source).next() {
            Some(DatasetSource::Root(src)) => src,
            _ => panic!("Failed to find source definition"),
        };

        IngestTask {
            dataset_id: dataset_id.to_owned(),
            layout: layout,
            source: source,
            meta_chain: meta_chain,
            listener: listener,
        }
    }

    // Note: Can be called from multiple threads
    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        self.listener.begin();
        println!("{} {:?}", self.dataset_id, self.layout);
        println!("{:?}", self.source);
        unimplemented!();
    }

    fn maybe_download() {}
}
