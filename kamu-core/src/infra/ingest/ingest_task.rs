use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};

pub struct IngestTask {
    dataset_id: DatasetIDBuf,
    layout: DatasetLayout,
    meta_chain: Box<dyn MetadataChain>,
    source: DatasetSourceRoot,
    vocab: DatasetVocabulary,
    listener: Arc<Mutex<dyn IngestListener>>,
    checkpointing_executor: CheckpointingExecutor,
    fetch_service: FetchService,
    prep_service: PrepService,
    read_service: ReadService,
}

impl IngestTask {
    pub fn new<'a>(
        dataset_id: &DatasetID,
        layout: DatasetLayout,
        meta_chain: Box<dyn MetadataChain>,
        vocab: DatasetVocabulary,
        listener: Arc<Mutex<dyn IngestListener>>,
        engine_factory: Arc<Mutex<EngineFactory>>,
    ) -> Self {
        // TODO: this is expensive
        let source = match meta_chain.iter_blocks().filter_map(|b| b.source).next() {
            Some(DatasetSource::Root(src)) => src,
            _ => panic!("Failed to find source definition"),
        };

        Self {
            dataset_id: dataset_id.to_owned(),
            layout: layout,
            meta_chain: meta_chain,
            source: source,
            vocab: vocab,
            listener: listener,
            checkpointing_executor: CheckpointingExecutor::new(),
            fetch_service: FetchService::new(),
            prep_service: PrepService::new(),
            read_service: ReadService::new(engine_factory),
        }
    }

    // Note: Can be called from multiple threads
    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        self.listener.lock().unwrap().begin();

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        let prev_hash = self.meta_chain.read_ref(&BlockRef::Head).unwrap();

        let fetch_result = self.maybe_fetch()?;
        let source_event_time = fetch_result.checkpoint.source_event_time.clone();

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Prepare, 0, 1);

        let prepare_result = self.maybe_prepare(fetch_result)?;

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Read, 0, 1);

        let read_result = self.maybe_read(prepare_result, source_event_time)?;

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Preprocess, 0, 1);

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Merge, 0, 1);

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Commit, 0, 1);

        let commit = self.maybe_commit(read_result, prev_hash)?;

        let res = match commit {
            None => IngestResult::UpToDate,
            Some(hash) => IngestResult::Updated { block_hash: hash },
        };

        Ok(res)
    }

    fn maybe_fetch(&mut self) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("fetch.yaml");

        self.checkpointing_executor
            .execute(&checkpoint_path, |old_checkpoint| {
                self.fetch_service.fetch(
                    &self.source.fetch,
                    old_checkpoint,
                    &self.layout.cache_dir.join("fetched.bin"),
                    Some(&mut FetchProgressListenerBridge {
                        listener: self.listener.clone(),
                    }),
                )
            })
            .map_err(|e| IngestError::internal(e))?
    }

    fn maybe_prepare(
        &mut self,
        fetch_result: ExecutionResult<FetchCheckpoint>,
    ) -> Result<ExecutionResult<PrepCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("prep.yaml");

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                |old_checkpoint: Option<PrepCheckpoint>| {
                    if let Some(ref cp) = old_checkpoint {
                        if cp.for_fetched_at == fetch_result.checkpoint.last_fetched {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    let null_steps = Vec::new();
                    let prep_steps = self.source.prepare.as_ref().unwrap_or(&null_steps);
                    self.prep_service.prepare(
                        prep_steps,
                        fetch_result.checkpoint.last_fetched,
                        old_checkpoint,
                        &self.layout.cache_dir.join("fetched.bin"),
                        &self.layout.cache_dir.join("prepared.bin"),
                    )
                },
            )
            .map_err(|e| IngestError::internal(e))?
    }

    fn maybe_read(
        &mut self,
        prep_result: ExecutionResult<PrepCheckpoint>,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<ExecutionResult<ReadCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("read.yaml");

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                |old_checkpoint: Option<ReadCheckpoint>| {
                    if let Some(ref cp) = old_checkpoint {
                        if cp.for_prepared_at == prep_result.checkpoint.last_prepared {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    self.read_service.read(
                        &self.dataset_id,
                        &self.layout,
                        &self.source,
                        source_event_time,
                        &self.vocab,
                        prep_result.checkpoint.last_prepared,
                        old_checkpoint,
                        &self.layout.cache_dir.join("prepared.bin"),
                    )
                },
            )
            .map_err(|e| IngestError::internal(e))?
    }

    fn maybe_commit(
        &mut self,
        read_result: ExecutionResult<ReadCheckpoint>,
        prev_hash: String,
    ) -> Result<Option<String>, IngestError> {
        // TODO: Atomicity
        if !read_result.was_up_to_date {
            let new_block = MetadataBlock {
                prev_block_hash: prev_hash,
                ..read_result.checkpoint.last_block
            };
            let hash = self.meta_chain.append(new_block);
            Ok(Some(hash))
        } else {
            Ok(None)
        }
    }
}

struct FetchProgressListenerBridge {
    listener: Arc<Mutex<dyn IngestListener>>,
}

impl FetchProgressListener for FetchProgressListenerBridge {
    fn on_progress(&mut self, progress: &FetchProgress) {
        self.listener.lock().unwrap().on_stage_progress(
            IngestStage::Fetch,
            progress.fetched_bytes,
            progress.total_bytes,
        );
    }
}
