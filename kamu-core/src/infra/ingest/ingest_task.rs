use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use chrono::Utc;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

pub struct IngestTask {
    dataset_id: DatasetIDBuf,
    layout: DatasetLayout,
    source: DatasetSourceRoot,
    listener: Arc<Mutex<dyn IngestListener>>,
    checkpointing_executor: CheckpointingExecutor,
    fetch_service: FetchService,
    prep_service: PrepService,
}

impl IngestTask {
    pub fn new<'a>(
        dataset_id: &DatasetID,
        layout: DatasetLayout,
        meta_chain: &'a dyn MetadataChain,
        listener: Arc<Mutex<dyn IngestListener>>,
    ) -> Self {
        // TODO: this is expensive
        let source = match meta_chain.iter_blocks().filter_map(|b| b.source).next() {
            Some(DatasetSource::Root(src)) => src,
            _ => panic!("Failed to find source definition"),
        };

        Self {
            dataset_id: dataset_id.to_owned(),
            layout: layout,
            source: source,
            listener: listener,
            checkpointing_executor: CheckpointingExecutor::new(),
            fetch_service: FetchService::new(),
            prep_service: PrepService::new(),
        }
    }

    // Note: Can be called from multiple threads
    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        {
            let mut listener = self.listener.lock().unwrap();
            listener.begin();
            listener.on_stage_progress(IngestStage::CheckCache, 0, 1);
        }

        let fetch_result = self.maybe_fetch()?;
        let prepare_result = self.maybe_prepare(&fetch_result)?;
        Ok(IngestResult::UpToDate)
    }

    fn maybe_fetch(&mut self) -> Result<ExecutionResult<FetchCheckpoint>, FetchError> {
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
            .map_err(|e| FetchError::internal(e))?
    }

    fn maybe_prepare(
        &mut self,
        fetch_result: &ExecutionResult<FetchCheckpoint>,
    ) -> Result<ExecutionResult<PrepCheckpoint>, PrepError> {
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
                        old_checkpoint,
                        &self.layout.cache_dir.join("prepared.bin"),
                    )
                },
            )
            .map_err(|e| PrepError::internal(e))?
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
