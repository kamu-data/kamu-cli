use crate::domain::*;

use std::cell::RefCell;
use std::collections::HashMap;
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

    fn get_datasets_ordered_by_depth<'i>(
        &self,
        starting_dataset_ids: impl Iterator<Item = &'i DatasetID>,
    ) -> Vec<(DatasetIDBuf, i32)> {
        let mut labeled = HashMap::new();
        for id in starting_dataset_ids {
            self.depth_first_label(id, &mut labeled);
        }
        let mut ordered = Vec::with_capacity(labeled.len());
        ordered.extend(labeled.into_iter());
        ordered.sort_by(|(a_id, a_depth), (b_id, b_depth)| {
            if a_depth == b_depth {
                a_id.cmp(&b_id)
            } else {
                a_depth.cmp(&b_depth)
            }
        });
        ordered
    }

    fn depth_first_label(
        &self,
        dataset_id: &DatasetID,
        labeled: &mut HashMap<DatasetIDBuf, i32>,
    ) -> i32 {
        if let Some(v) = labeled.get(dataset_id) {
            return *v;
        }

        let summary = self.metadata_repo.borrow().get_summary(dataset_id).unwrap();

        let depth = summary
            .dependencies
            .iter()
            .map(|id| self.depth_first_label(id, labeled))
            .max()
            .map(|max| max + 1)
            .unwrap_or(0);

        labeled.insert(dataset_id.to_owned(), depth);
        depth
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

    fn slice<'a>(
        &self,
        to_slice: &'a [(DatasetIDBuf, i32)],
    ) -> (i32, &'a [(DatasetIDBuf, i32)], &'a [(DatasetIDBuf, i32)]) {
        let d = to_slice.get(0).unwrap().1;
        let count = to_slice.iter().take_while(|(_, depth)| *depth == d).count();
        (d, &to_slice[..count], &to_slice[count..])
    }
}

impl PullService for PullServiceImpl {
    fn pull_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        recursive: bool,
        all: bool,
        mut ingest_listener: Option<&mut dyn IngestMultiListener>,
        mut transform_listener: Option<&mut dyn TransformMultiListener>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let metadata_repo = self.metadata_repo.borrow();

        let starting_dataset_ids: std::collections::HashSet<DatasetIDBuf> = if !all {
            dataset_ids.map(|id| id.to_owned()).collect()
        } else {
            metadata_repo.get_all_datasets().collect()
        };

        let datasets_labeled = self
            .get_datasets_ordered_by_depth(&mut starting_dataset_ids.iter().map(|id| id.as_ref()));

        let datasets_to_pull = if recursive || all {
            datasets_labeled
        } else {
            datasets_labeled
                .into_iter()
                .filter(|(id, _)| starting_dataset_ids.contains(id))
                .collect()
        };

        let mut results = Vec::with_capacity(datasets_to_pull.len());

        let mut rest = &datasets_to_pull[..];
        while !rest.is_empty() {
            let (depth, level, tail) = self.slice(rest);
            rest = tail;

            // See: https://internals.rust-lang.org/t/should-option-mut-t-implement-copy/3715/6
            // For listener option magic explanation
            let results_level: Vec<_> = if depth == 0 {
                self.ingest_svc
                    .borrow_mut()
                    .ingest_multi(
                        &mut level.iter().map(|(id, _)| id.as_ref()),
                        ingest_listener.as_mut().map_or(None, |l| Some(*l)),
                    )
                    .into_iter()
                    .map(|(id, res)| (id, Self::convert_ingest_result(res)))
                    .collect()
            } else {
                self.transform_svc
                    .borrow_mut()
                    .transform_multi(
                        &mut level.iter().map(|(id, _)| id.as_ref()),
                        transform_listener.as_mut().map_or(None, |l| Some(*l)),
                    )
                    .into_iter()
                    .map(|(id, res)| (id, Self::convert_transform_result(res)))
                    .collect()
            };

            let errors = results_level.iter().any(|(_, r)| r.is_err());
            results.extend(results_level);
            if errors {
                break;
            }
        }

        results
    }
}
