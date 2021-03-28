use crate::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use slog::{info, Logger};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct PullServiceImpl {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    ingest_svc: Rc<RefCell<dyn IngestService>>,
    transform_svc: Rc<RefCell<dyn TransformService>>,
    logger: Logger,
}

impl PullServiceImpl {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        ingest_svc: Rc<RefCell<dyn IngestService>>,
        transform_svc: Rc<RefCell<dyn TransformService>>,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            ingest_svc: ingest_svc,
            transform_svc: transform_svc,
            logger: logger,
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
                IngestResult::UpToDate { uncacheable: _ } => PullResult::UpToDate,
                IngestResult::Updated {
                    block_hash,
                    has_more: _,
                    uncacheable: _,
                } => PullResult::Updated {
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
        options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let starting_dataset_ids: std::collections::HashSet<DatasetIDBuf> = if !options.all {
            dataset_ids.map(|id| id.to_owned()).collect()
        } else {
            self.metadata_repo.borrow().get_all_datasets().collect()
        };

        info!(self.logger, "Performing pull_multi"; "datasets" => ?starting_dataset_ids);

        let datasets_labeled = self
            .get_datasets_ordered_by_depth(&mut starting_dataset_ids.iter().map(|id| id.as_ref()));

        let datasets_to_pull = if options.recursive || options.all {
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

            let results_level: Vec<_> = if depth == 0 {
                self.ingest_svc
                    .borrow_mut()
                    .ingest_multi(
                        &mut level.iter().map(|(id, _)| id.as_ref()),
                        options.ingest_options.clone(),
                        ingest_listener.clone(),
                    )
                    .into_iter()
                    .map(|(id, res)| (id, Self::convert_ingest_result(res)))
                    .collect()
            } else {
                self.transform_svc
                    .borrow_mut()
                    .transform_multi(
                        &mut level.iter().map(|(id, _)| id.as_ref()),
                        transform_listener.clone(),
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

    fn set_watermark(
        &mut self,
        dataset_id: &DatasetID,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        let mut chain = self
            .metadata_repo
            .borrow_mut()
            .get_metadata_chain(dataset_id)?;

        if let Some(last_watermark) = chain
            .iter_blocks()
            .filter_map(|b| b.output_watermark)
            .next()
        {
            if last_watermark >= watermark {
                return Ok(PullResult::UpToDate);
            }
        }

        let last_hash = chain.read_ref(&BlockRef::Head);

        let new_block = MetadataBlock {
            block_hash: Sha3_256::zero(),
            prev_block_hash: last_hash,
            system_time: Utc::now(),
            output_slice: None,
            output_watermark: Some(watermark),
            input_slices: None,
            source: None,
            vocab: None,
        };

        let new_hash = chain.append(new_block);
        Ok(PullResult::Updated {
            block_hash: new_hash,
        })
    }
}
