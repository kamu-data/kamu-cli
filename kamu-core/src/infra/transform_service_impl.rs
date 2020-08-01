use crate::domain::*;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct TransformServiceImpl {
    //metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
}

impl TransformServiceImpl {
    pub fn new(_metadata_repo: Rc<RefCell<dyn MetadataRepository>>) -> Self {
        Self {
            //metadata_repo: metadata_repo,
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform(
        _dataset_id: &DatasetID,
        _listener: Arc<Mutex<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }
}

impl TransformService for TransformServiceImpl {
    fn transform(
        &mut self,
        dataset_id: &DatasetID,
        maybe_listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<TransformResult, TransformError> {
        let null_listener = Arc::new(Mutex::new(NullTransformListener {}));
        let listener = maybe_listener.unwrap_or(null_listener);

        Self::do_transform(dataset_id, listener)
    }

    fn transform_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        maybe_multi_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> {
        let null_multi_listener = Arc::new(Mutex::new(NullTransformMultiListener {}));
        let multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let thread_handles: Vec<_> = dataset_ids
            .map(|id_ref| {
                let id = id_ref.to_owned();
                let null_listener = Arc::new(Mutex::new(NullTransformListener {}));
                let listener = multi_listener
                    .lock()
                    .unwrap()
                    .begin_transform(&id)
                    .unwrap_or(null_listener);

                std::thread::Builder::new()
                    .name("transform_multi".to_owned())
                    .spawn(move || {
                        let res = Self::do_transform(&id, listener);
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
