use crate::domain::*;

use std::cell::RefCell;
use std::rc::Rc;

pub struct IngestServiceImpl {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
}

impl IngestServiceImpl {
    pub fn new(metadata_repo: Rc<RefCell<dyn MetadataRepository>>) -> Self {
        Self {
            metadata_repo: metadata_repo,
        }
    }

    // Note: Can be called from multiple threads
    fn do_ingest(
        dataset_id: &DatasetID,
        listener: &mut dyn IngestListener,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }
}

impl IngestService for IngestServiceImpl {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        maybe_listener: Option<Box<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener = Box::new(NullIngestListener {});
        let mut listener = maybe_listener.unwrap_or(null_listener);

        Self::do_ingest(dataset_id, listener.as_mut())
    }

    fn ingest_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        maybe_multi_listener: Option<&mut dyn IngestMultiListener>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)> {
        let mut null_multi_listener = NullIngestMultiListener {};
        let multi_listener = maybe_multi_listener.unwrap_or(&mut null_multi_listener);

        let thread_handles: Vec<_> = dataset_ids
            .map(|id_ref| {
                let id = id_ref.to_owned();
                let null_listener = Box::new(NullIngestListener {});
                let mut listener = multi_listener.begin_ingest(&id).unwrap_or(null_listener);
                std::thread::Builder::new()
                    .name("ingest_multi".to_owned())
                    .spawn(move || {
                        let res = Self::do_ingest(&id, listener.as_mut());
                        (id, res)
                    })
                    .unwrap()
            })
            .collect();

        thread_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    }
}
