use super::ingest::*;
use crate::domain::*;
use crate::infra::*;
use dill::*;
use opendatafabric::*;

use slog::{info, o, Logger};
use std::sync::{Arc, Mutex};

pub struct IngestServiceImpl {
    volume_layout: VolumeLayout,
    metadata_repo: Arc<dyn MetadataRepository>,
    engine_factory: Arc<dyn EngineFactory>,
    logger: Logger,
}

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        volume_layout: &VolumeLayout,
        metadata_repo: Arc<dyn MetadataRepository>,
        engine_factory: Arc<dyn EngineFactory>,
        logger: Logger,
    ) -> Self {
        Self {
            volume_layout: volume_layout.clone(),
            metadata_repo,
            engine_factory,
            logger,
        }
    }

    // TODO: error handling
    fn get_dataset_layout(&self, dataset_id: &DatasetID) -> DatasetLayout {
        DatasetLayout::create(&self.volume_layout, dataset_id).unwrap()
    }

    // TODO: Introduce intermediate structs to avoid full unpacking
    fn merge_results(
        combined_result: Option<IngestResult>,
        new_result: IngestResult,
    ) -> IngestResult {
        if let None = combined_result {
            return new_result;
        }

        if let IngestResult::UpToDate { .. } = new_result {
            return combined_result.unwrap();
        }

        if let Some(IngestResult::Updated {
            old_head: prev_old_head,
            new_head: _,
            num_blocks: prev_num_blocks,
            has_more: _,
            uncacheable: _,
        }) = combined_result
        {
            if let IngestResult::Updated {
                old_head: _,
                new_head: new_new_head,
                num_blocks: new_num_blocks,
                has_more: new_has_more,
                uncacheable: new_uncacheable,
            } = new_result
            {
                return IngestResult::Updated {
                    old_head: prev_old_head,
                    new_head: new_new_head,
                    num_blocks: prev_num_blocks + new_num_blocks,
                    has_more: new_has_more,
                    uncacheable: new_uncacheable,
                };
            }
        }

        unreachable!()
    }
}

impl IngestService for IngestServiceImpl {
    fn ingest(
        &self,
        dataset_id: &DatasetID,
        options: IngestOptions,
        maybe_listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener: Arc<Mutex<dyn IngestListener>> =
            Arc::new(Mutex::new(NullIngestListener {}));
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(self.logger, "Ingesting single dataset"; "dataset" => dataset_id.as_str());

        let meta_chain = self.metadata_repo.get_metadata_chain(dataset_id).unwrap();

        let layout = self.get_dataset_layout(dataset_id);

        let logger = self.logger.new(o!("dataset" => dataset_id.to_string()));

        let mut ingest_task = IngestTask::new(
            dataset_id,
            options,
            layout,
            meta_chain,
            None,
            listener,
            self.engine_factory.clone(),
            logger,
        );

        ingest_task.ingest()
    }

    fn ingest_from(
        &self,
        dataset_id: &DatasetID,
        fetch: FetchStep,
        options: IngestOptions,
        maybe_listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener: Arc<Mutex<dyn IngestListener>> =
            Arc::new(Mutex::new(NullIngestListener {}));
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(self.logger, "Ingesting single dataset from overriden source"; "dataset" => dataset_id.as_str(), "fetch" => ?fetch);

        let meta_chain = self.metadata_repo.get_metadata_chain(dataset_id).unwrap();

        let layout = self.get_dataset_layout(dataset_id);

        let logger = self.logger.new(o!("dataset" => dataset_id.to_string()));

        let mut ingest_task = IngestTask::new(
            dataset_id,
            options,
            layout,
            meta_chain,
            Some(fetch),
            listener,
            self.engine_factory.clone(),
            logger,
        );

        ingest_task.ingest()
    }

    fn ingest_multi(
        &self,
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
                let meta_chain = self.metadata_repo.get_metadata_chain(&id).unwrap();
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
                            None,
                            listener,
                            engine_factory,
                            logger,
                        );

                        let mut combined_result = None;
                        loop {
                            match ingest_task.ingest() {
                                Ok(res) => {
                                    combined_result =
                                        Some(Self::merge_results(combined_result, res));

                                    if let Some(IngestResult::Updated { has_more, .. }) =
                                        combined_result
                                    {
                                        if has_more && exhaust_sources {
                                            continue;
                                        }
                                    }
                                }
                                Err(e) => return (id, Err(e)),
                            }
                            break;
                        }
                        (id, Ok(combined_result.unwrap()))
                    })
                    .unwrap()
            })
            .collect();

        let results: Vec<_> = thread_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        results
    }
}
