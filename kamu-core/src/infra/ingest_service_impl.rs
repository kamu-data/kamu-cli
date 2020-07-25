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
        let scale: f32 = rand::random();

        listener.begin();

        std::thread::sleep_ms((2000 as f32 * scale) as u32);
        listener.on_stage_progress(IngestStage::CheckCache, 0, 0);
        std::thread::sleep_ms((2000 as f32 * scale) as u32);

        let chance: f32 = rand::random();
        if rand::random::<f32>() < 0.3 {
            let error = IngestError::FetchError;
            listener.error(IngestStage::CheckCache, &error);
            Err(error)
        } else if rand::random::<f32>() < 0.3 {
            let result = IngestResult::UpToDate;
            listener.success(&result);
            Ok(result)
        } else {
            for i in 0..100 {
                listener.on_stage_progress(IngestStage::Fetch, i, 100);
                std::thread::sleep_ms((100 as f32 * scale) as u32);
            }

            if rand::random::<f32>() < 0.3 {
                let error = IngestError::FetchError;
                listener.error(IngestStage::Fetch, &error);
                return Err(error);
            }

            listener.on_stage_progress(IngestStage::Fetch, 100, 100);

            std::thread::sleep_ms((1000 as f32 * scale) as u32);
            listener.on_stage_progress(IngestStage::Prepare, 0, 0);

            std::thread::sleep_ms((1000 as f32 * scale) as u32);
            listener.on_stage_progress(IngestStage::Read, 0, 0);

            std::thread::sleep_ms((1000 as f32 * scale) as u32);
            listener.on_stage_progress(IngestStage::Preprocess, 0, 0);

            std::thread::sleep_ms((1000 as f32 * scale) as u32);
            listener.on_stage_progress(IngestStage::Merge, 0, 0);

            std::thread::sleep_ms((1000 as f32 * scale) as u32);
            listener.on_stage_progress(IngestStage::Commit, 0, 0);

            std::thread::sleep_ms(100);

            let result = IngestResult::Updated {
                block_hash: "13127948719dsdka1203ahsjkdh12983213".to_owned(),
            };
            listener.success(&result);
            Ok(result)
        }
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
        maybe_multi_listener: Option<Box<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)> {
        let null_multi_listener = Box::new(NullIngestMultiListener {});
        let mut multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let thread_handles: Vec<_> = dataset_ids
            .map(|id_ref| {
                let id = id_ref.to_owned();
                let null_listener = Box::new(NullIngestListener {});
                let mut listener = multi_listener.begin_ingest(&id).unwrap_or(null_listener);
                std::thread::spawn(move || {
                    let res = Self::do_ingest(&id, listener.as_mut());
                    (id, res)
                })
            })
            .collect();

        thread_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    }
}
