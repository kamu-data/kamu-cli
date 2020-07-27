use crate::domain::*;

use std::cell::RefCell;
use std::rc::Rc;

pub struct PullServiceImpl {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    ingest_svc: Rc<RefCell<dyn IngestService>>,
    transform_svc: Rc<RefCell<dyn TransformService>>,
}

impl PullServiceImpl {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        ingest_svc: Rc<RefCell<dyn IngestService>>,
        transform_svc: Rc<RefCell<dyn TransformService>>,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            ingest_svc: ingest_svc,
            transform_svc: transform_svc,
        }
    }

    fn convert_ingest_result(
        res: Result<IngestResult, IngestError>,
    ) -> Result<PullResult, PullError> {
        match res {
            Ok(res) => Ok(match res {
                IngestResult::UpToDate => PullResult::UpToDate,
                IngestResult::Updated { block_hash } => PullResult::Updated {
                    block_hash: block_hash,
                },
            }),
            Err(err) => Err(err.into()),
        }
    }

    fn convert_transform_result(
        res: Result<TransformResult, TransformError>,
    ) -> Result<PullResult, PullError> {
        match res {
            Ok(res) => Ok(match res {
                TransformResult::UpToDate => PullResult::UpToDate,
                TransformResult::Updated { block_hash } => PullResult::Updated {
                    block_hash: block_hash,
                },
            }),
            Err(err) => Err(err.into()),
        }
    }
}

impl PullService for PullServiceImpl {
    fn pull_multi(
        &mut self,
        dataset_ids_iter: &mut dyn Iterator<Item = &DatasetID>,
        recursive: bool,
        all: bool,
        ingest_listener: Option<Box<dyn IngestMultiListener>>,
        transform_listener: Option<Box<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let dataset_ids: Vec<DatasetIDBuf> = if all {
            self.metadata_repo.borrow().get_all_datasets().collect()
        } else if recursive {
            unimplemented!()
        } else {
            dataset_ids_iter.map(|id| id.to_owned()).collect()
        };

        let mut results: Vec<_> = Vec::new();

        /*results.extend(
            self.ingest_svc
                .borrow_mut()
                .ingest_multi(
                    &mut dataset_ids.iter().map(|id| id.as_ref()),
                    ingest_listener,
                )
                .into_iter()
                .map(|(id, res)| (id, Self::convert_ingest_result(res))),
        );

        results.extend(
            self.transform_svc
                .borrow_mut()
                .transform_multi(
                    &mut dataset_ids.iter().map(|id| id.as_ref()),
                    transform_listener,
                )
                .into_iter()
                .map(|(id, res)| (id, Self::convert_transform_result(res))),
        );*/

        results
    }
}
