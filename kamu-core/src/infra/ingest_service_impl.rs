use super::ingest::*;
use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use slog::{info, o, Logger};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct IngestServiceImpl {
    volume_layout: VolumeLayout,
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    engine_factory: Arc<Mutex<EngineFactory>>,
    logger: Logger,
}

impl IngestServiceImpl {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        engine_factory: Arc<Mutex<EngineFactory>>,
        volume_layout: &VolumeLayout,
        logger: Logger,
    ) -> Self {
        Self {
            volume_layout: volume_layout.clone(),
            metadata_repo: metadata_repo,
            engine_factory: engine_factory,
            logger: logger,
        }
    }

    // TODO: error handling
    fn get_dataset_layout(&self, dataset_id: &DatasetID) -> DatasetLayout {
        DatasetLayout::create(&self.volume_layout, dataset_id).unwrap()
    }

    fn update_summary(
        &self,
        dataset_id: &DatasetID,
        result: &IngestResult,
    ) -> Result<(), IngestError> {
        match result {
            IngestResult::UpToDate => Ok(()),
            IngestResult::Updated {
                block_hash,
                has_more: _,
            } => {
                let mut metadata_repo = self.metadata_repo.borrow_mut();

                let mut summary = metadata_repo
                    .get_summary(dataset_id)
                    .map_err(|e| IngestError::internal(e))?;

                let block = metadata_repo
                    .get_metadata_chain(dataset_id)
                    .unwrap()
                    .get_block(block_hash)
                    .unwrap();

                summary.num_records = match block.output_slice {
                    Some(slice) => summary.num_records + slice.num_records as u64,
                    _ => 0,
                };

                summary.last_pulled = Some(block.system_time);

                let layout = DatasetLayout::new(&self.volume_layout, dataset_id);
                summary.data_size = fs_extra::dir::get_size(layout.data_dir).unwrap_or(0);
                summary.data_size += fs_extra::dir::get_size(layout.checkpoints_dir).unwrap_or(0);

                metadata_repo
                    .update_summary(dataset_id, summary)
                    .map_err(|e| IngestError::internal(e))
            }
        }
    }
}

impl IngestService for IngestServiceImpl {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        options: IngestOptions,
        maybe_listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener: Arc<Mutex<dyn IngestListener>> =
            Arc::new(Mutex::new(NullIngestListener {}));
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(self.logger, "Ingesting single dataset"; "dataset" => dataset_id.as_str());

        let meta_chain = self
            .metadata_repo
            .borrow()
            .get_metadata_chain(dataset_id)
            .unwrap();

        let layout = self.get_dataset_layout(dataset_id);

        let logger = self.logger.new(o!("dataset" => dataset_id.to_string()));

        let mut ingest_task = IngestTask::new(
            dataset_id,
            options,
            layout,
            meta_chain,
            listener,
            self.engine_factory.clone(),
            logger,
        );

        let result = ingest_task.ingest()?;
        self.update_summary(dataset_id, &result)?;
        Ok(result)
    }

    fn ingest_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)> {
        let null_multi_listener: Arc<Mutex<dyn IngestMultiListener>> =
            Arc::new(Mutex::new(NullIngestMultiListener {}));
        let multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let dataset_ids_owned: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        info!(self.logger, "Ingesting multiple datasets"; "datasets" => ?dataset_ids_owned);

        let thread_handles: Vec<_> = dataset_ids_owned
            .into_iter()
            .map(|id| {
                let layout = self.get_dataset_layout(&id);
                let meta_chain = self.metadata_repo.borrow().get_metadata_chain(&id).unwrap();
                let engine_factory = self.engine_factory.clone();
                let task_options = options.clone();

                let null_listener = Arc::new(Mutex::new(NullIngestListener {}));
                let listener = multi_listener
                    .lock()
                    .unwrap()
                    .begin_ingest(&id)
                    .unwrap_or(null_listener);

                let logger = self.logger.new(o!("dataset" => id.to_string()));

                std::thread::Builder::new()
                    .name("ingest_multi".to_owned())
                    .spawn(move || {
                        let exhaust_sources = task_options.exhaust_sources;

                        let mut ingest_task = IngestTask::new(
                            &id,
                            task_options,
                            layout,
                            meta_chain,
                            listener,
                            engine_factory,
                            logger,
                        );

                        let mut results = Vec::new();
                        loop {
                            let res = ingest_task.ingest();
                            let has_more = match res {
                                Ok(IngestResult::Updated { has_more, .. }) => {
                                    has_more && exhaust_sources
                                }
                                _ => false,
                            };
                            results.push((id.clone(), res));
                            if !has_more {
                                break;
                            }
                        }
                        results
                    })
                    .unwrap()
            })
            .collect();

        let results: Vec<_> = thread_handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        results
            .iter()
            .filter(|(_, res)| res.is_ok())
            .for_each(|(dataset_id, res)| {
                self.update_summary(dataset_id, res.as_ref().unwrap())
                    .unwrap()
            });

        results
    }
}
