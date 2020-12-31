use super::*;
use crate::domain::*;
use crate::infra::*;
use opendatafabric::serde::yaml::formats::datetime_rfc3339;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use slog::{info, o, Logger};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

pub struct IngestTask {
    dataset_id: DatasetIDBuf,
    options: IngestOptions,
    layout: DatasetLayout,
    meta_chain: RefCell<Box<dyn MetadataChain>>,
    source: DatasetSourceRoot,
    prev_checkpoint: Option<Sha3_256>,
    vocab: DatasetVocabulary,
    listener: Arc<Mutex<dyn IngestListener>>,
    checkpointing_executor: CheckpointingExecutor,
    fetch_service: FetchService,
    prep_service: PrepService,
    read_service: ReadService,
    logger: Logger,
}

impl IngestTask {
    pub fn new<'a>(
        dataset_id: &DatasetID,
        options: IngestOptions,
        layout: DatasetLayout,
        meta_chain: Box<dyn MetadataChain>,
        listener: Arc<Mutex<dyn IngestListener>>,
        engine_factory: Arc<Mutex<EngineFactory>>,
        logger: Logger,
    ) -> Self {
        // TODO: PERF: This is expensive and could be cached
        let mut source = None;
        let mut prev_checkpoint = None;
        let mut vocab = None;
        for block in meta_chain.iter_blocks() {
            if source.is_none() {
                match block.source {
                    Some(DatasetSource::Root(src)) => source = Some(src),
                    _ => (),
                }
            }
            // TODO: Verify assumption that only blocks with output_slice have checkpoints
            if prev_checkpoint.is_none() && block.output_slice.is_some() {
                prev_checkpoint = Some(block.block_hash)
            }
            if vocab.is_none() && block.vocab.is_some() {
                vocab = block.vocab;
            }

            if source.is_some() && vocab.is_some() && prev_checkpoint.is_some() {
                break;
            }
        }

        if source.is_none() {
            panic!("Failed to find source definition");
        }

        Self {
            dataset_id: dataset_id.to_owned(),
            options: options,
            layout: layout,
            meta_chain: RefCell::new(meta_chain),
            source: source.unwrap(),
            prev_checkpoint: prev_checkpoint,
            vocab: vocab.unwrap_or_default(),
            listener: listener,
            checkpointing_executor: CheckpointingExecutor::new(),
            fetch_service: FetchService::new(logger.new(o!())),
            prep_service: PrepService::new(),
            read_service: ReadService::new(engine_factory),
            logger: logger,
        }
    }

    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        self.listener.lock().unwrap().begin();

        match self.ingest_inner() {
            Ok((res, cacheable)) => {
                if cacheable {
                    self.listener.lock().unwrap().success(&res);
                } else {
                    self.listener.lock().unwrap().uncacheable();
                }
                Ok(res)
            }
            Err(err) => {
                self.listener.lock().unwrap().error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    pub fn ingest_inner(&mut self) -> Result<(IngestResult, bool), IngestError> {
        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        let prev_hash = self.meta_chain.borrow().read_ref(&BlockRef::Head).unwrap();

        let fetch_result = self.maybe_fetch()?;
        let has_more = fetch_result.checkpoint.has_more;
        let cacheable = fetch_result.checkpoint.is_cacheable();
        let source_event_time = fetch_result.checkpoint.source_event_time.clone();

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Prepare, 0, 1);

        let prepare_result = self.maybe_prepare(fetch_result.clone())?;

        self.listener
            .lock()
            .unwrap()
            .on_stage_progress(IngestStage::Read, 0, 1);

        let read_result = self.maybe_read(prepare_result, source_event_time)?;

        {
            let mut l = self.listener.lock().unwrap();
            l.on_stage_progress(IngestStage::Preprocess, 0, 1);
            l.on_stage_progress(IngestStage::Merge, 0, 1);
            l.on_stage_progress(IngestStage::Commit, 0, 1);
        }

        let commit_result = self.maybe_commit(fetch_result, read_result, prev_hash)?;

        let res = if commit_result.was_up_to_date {
            IngestResult::UpToDate
        } else {
            IngestResult::Updated {
                block_hash: commit_result.checkpoint.last_hash,
                has_more: has_more,
            }
        };

        Ok((res, cacheable))
    }

    fn maybe_fetch(&mut self) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("fetch.yaml");

        let commit_checkpoint_path = self.layout.cache_dir.join("commit.yaml");
        let commit_checkpoint = self
            .checkpointing_executor
            .read_checkpoint::<CommitCheckpoint>(&commit_checkpoint_path, "CommitCheckpoint")
            .map_err(|e| IngestError::internal(e))?;

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                "FetchCheckpoint",
                |old_checkpoint: Option<FetchCheckpoint>| {
                    if let Some(ref cp) = old_checkpoint {
                        if !cp.is_cacheable() && !self.options.force_uncacheable {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }

                        // This is needed to make sure that for sources that yield multiple
                        // data files (e.g. `filesGlob`) we will not ask for the next file
                        // until we fully ingested the previous one. We guard against cases
                        // when data was fetch but never committed or when processing failed
                        // or was aborter before the commit.
                        if commit_checkpoint.is_none()
                            || commit_checkpoint.unwrap().for_fetched_at != cp.last_fetched
                        {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    self.fetch_service.fetch(
                        &self.source.fetch,
                        old_checkpoint,
                        &self.layout.cache_dir.join("fetched.bin"),
                        Some(&mut FetchProgressListenerBridge {
                            listener: self.listener.clone(),
                        }),
                    )
                },
            )
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
                "PrepCheckpoint",
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
                "ReadCheckpoint",
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
                        self.prev_checkpoint,
                        &self.vocab,
                        source_event_time,
                        prep_result.checkpoint.last_prepared,
                        old_checkpoint,
                        &self.layout.cache_dir.join("prepared.bin"),
                        self.listener.clone(),
                    )
                },
            )
            .map_err(|e| IngestError::internal(e))?
    }

    // TODO: Atomicity
    fn maybe_commit(
        &mut self,
        fetch_result: ExecutionResult<FetchCheckpoint>,
        read_result: ExecutionResult<ReadCheckpoint>,
        prev_hash: Sha3_256,
    ) -> Result<ExecutionResult<CommitCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("commit.yaml");

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                "CommitCheckpoint",
                |old_checkpoint: Option<CommitCheckpoint>| {
                    if let Some(ref cp) = old_checkpoint {
                        if cp.for_read_at == read_result.checkpoint.last_read {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    let new_block = MetadataBlock {
                        prev_block_hash: Some(prev_hash),
                        ..read_result.checkpoint.last_block
                    };

                    let hash = self.meta_chain.borrow_mut().append(new_block);
                    info!(self.logger, "Committed new block"; "hash" => hash.to_string());

                    // TODO: Data should be moved before writing block file
                    std::fs::rename(
                        &read_result.checkpoint.out_data_path,
                        self.layout.data_dir.join(hash.to_string()),
                    )
                    .map_err(|e| IngestError::internal(e))?;

                    // TODO: Checkpoint should be moved before writing block file
                    std::fs::rename(
                        &read_result.checkpoint.new_checkpoint_dir,
                        self.layout.checkpoints_dir.join(hash.to_string()),
                    )
                    .map_err(|e| IngestError::internal(e))?;

                    Ok(ExecutionResult {
                        was_up_to_date: false,
                        checkpoint: CommitCheckpoint {
                            last_committed: Utc::now(),
                            for_read_at: read_result.checkpoint.last_read,
                            for_fetched_at: fetch_result.checkpoint.last_fetched,
                            last_hash: hash,
                        },
                    })
                },
            )
            .map_err(|e| IngestError::internal(e))?
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

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_committed: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_read_at: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_fetched_at: DateTime<Utc>,
    pub last_hash: Sha3_256,
}
