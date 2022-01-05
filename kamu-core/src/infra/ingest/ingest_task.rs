// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use crate::infra::*;
use opendatafabric::serde::yaml::formats::datetime_rfc3339;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::cell::RefCell;
use std::sync::Arc;
use tracing::error;
use tracing::info;
use tracing::info_span;

pub struct IngestTask {
    options: IngestOptions,
    dataset_handle: DatasetHandle,
    next_offset: i64,
    layout: DatasetLayout,
    meta_chain: RefCell<Box<dyn MetadataChain>>,
    source: SetPollingSource,
    fetch_override: Option<FetchStep>,
    prev_checkpoint: Option<Multihash>,
    vocab: DatasetVocabulary,
    listener: Arc<dyn IngestListener>,
    checkpointing_executor: CheckpointingExecutor,
    fetch_service: FetchService,
    prep_service: PrepService,
    read_service: ReadService,
}

impl IngestTask {
    pub fn new<'a>(
        dataset_handle: DatasetHandle,
        options: IngestOptions,
        layout: DatasetLayout,
        meta_chain: Box<dyn MetadataChain>,
        fetch_override: Option<FetchStep>,
        listener: Arc<dyn IngestListener>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        // TODO: PERF: This is expensive and could be cached
        let mut source = None;
        let mut prev_checkpoint = None;
        let mut vocab = None;
        let mut next_offset = 0;

        for (block_hash, block) in meta_chain.iter_blocks() {
            match block.event {
                MetadataEvent::AddData(add_data) => {
                    if next_offset == 0 {
                        next_offset = add_data.output_data.interval.end + 1;
                    }
                    // TODO: Keep track of other types of blocks that may produce checkpoints
                    if prev_checkpoint.is_none() {
                        prev_checkpoint = Some(block_hash);
                    }
                }
                MetadataEvent::SetPollingSource(src) => {
                    if source.is_none() {
                        source = Some(src);
                    }
                }
                MetadataEvent::SetVocab(set_vocab) => {
                    vocab = Some(set_vocab.into());
                }
                MetadataEvent::ExecuteQuery(_)
                | MetadataEvent::Seed(_)
                | MetadataEvent::SetTransform(_)
                | MetadataEvent::SetWatermark(_) => (),
            }

            if source.is_some() && vocab.is_some() && prev_checkpoint.is_some() {
                break;
            }
        }

        let source = source.expect("Failed to find source definition");

        if fetch_override.is_some() {
            if let FetchStep::FilesGlob(_) = source.fetch {
                panic!("Fetch override use is not supported for glob sources, as globs maintain strict ordering and state")
            }
        }

        Self {
            options,
            dataset_handle,
            next_offset,
            layout,
            meta_chain: RefCell::new(meta_chain),
            source,
            fetch_override,
            prev_checkpoint,
            vocab: vocab.unwrap_or_default(),
            listener,
            checkpointing_executor: CheckpointingExecutor::new(),
            fetch_service: FetchService::new(),
            prep_service: PrepService::new(),
            read_service: ReadService::new(engine_provisioner),
        }
    }

    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        let span = info_span!("Ingesting data", dataset_handle = %self.dataset_handle);
        let _span_guard = span.enter();
        info!(source = ?self.source, prev_checkpoint = ?self.prev_checkpoint, vocab = ?self.vocab, "Ingest details");

        self.listener.begin();

        match self.ingest_inner() {
            Ok(res) => {
                info!("Ingest successful");
                self.listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                error!(error = ?err, "Ingest failed");
                self.listener.error(&err);
                Err(err)
            }
        }
    }

    pub fn ingest_inner(&mut self) -> Result<IngestResult, IngestError> {
        self.listener
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        let prev_hash = self.meta_chain.borrow().read_ref(&BlockRef::Head).unwrap();

        let fetch_result = self.maybe_fetch()?;
        let has_more = fetch_result.checkpoint.has_more;
        let cacheable = fetch_result.checkpoint.is_cacheable();
        let source_event_time = fetch_result.checkpoint.source_event_time.clone();

        self.listener.on_stage_progress(IngestStage::Prepare, 0, 1);

        let prepare_result = self.maybe_prepare(fetch_result.clone())?;

        self.listener.on_stage_progress(IngestStage::Read, 0, 1);

        let read_result = self.maybe_read(prepare_result, source_event_time)?;

        self.listener
            .on_stage_progress(IngestStage::Preprocess, 0, 1);
        self.listener.on_stage_progress(IngestStage::Merge, 0, 1);
        self.listener.on_stage_progress(IngestStage::Commit, 0, 1);

        let commit_result = self.maybe_commit(fetch_result, read_result, prev_hash.clone())?;

        let res = if commit_result.was_up_to_date || commit_result.checkpoint.last_hash.is_none() {
            IngestResult::UpToDate {
                uncacheable: !cacheable,
            }
        } else {
            IngestResult::Updated {
                old_head: prev_hash,
                new_head: commit_result.checkpoint.last_hash.unwrap(),
                num_blocks: 1,
                has_more: has_more,
                uncacheable: !cacheable,
            }
        };

        Ok(res)
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
                    // Ingesting from overridden source?
                    if let Some(fetch_override) = &self.fetch_override {
                        info!(fetch = ?fetch_override, "Fetching the overridden source");

                        return self
                            .fetch_service
                            .fetch(
                                fetch_override,
                                None,
                                &self.layout.cache_dir.join("fetched.bin"),
                                Some(&mut FetchProgressListenerBridge {
                                    listener: self.listener.clone(),
                                }),
                            )
                            .map(|r| ExecutionResult {
                                // Cache data for overridden source may influence future normal runs, so we filter it
                                checkpoint: FetchCheckpoint {
                                    last_modified: None,
                                    etag: None,
                                    ..r.checkpoint
                                },
                                ..r
                            });
                    }

                    if let Some(ref cp) = old_checkpoint {
                        if !cp.is_cacheable() && !self.options.force_uncacheable {
                            info!("Skipping fetch of uncacheable source");
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
                            info!("Skipping fetch to complete previous ingestion");
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    let span = info_span!("Fetching the data");
                    let _span_guard = span.enter();

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

    // TODO: PERF: Skip the copying if there are no prep steps
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

                    let span = info_span!("Preparing the data");
                    let _span_guard = span.enter();

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

                    let span = info_span!("Reading the data");
                    let _span_guard = span.enter();

                    self.read_service.read(
                        &self.dataset_handle,
                        &self.layout,
                        &self.source,
                        self.prev_checkpoint.clone(),
                        &self.vocab,
                        Utc::now(), // TODO: inject
                        source_event_time,
                        self.next_offset,
                        &self.layout.cache_dir.join("read.bin"),
                        &self.layout.cache_dir.join("read.checkpoint"),
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
        prev_hash: Multihash,
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

                    let data_path = self.layout.cache_dir.join("read.bin");
                    let checkpoint_dir = self.layout.cache_dir.join("read.checkpoint");
                    let data_interval = read_result.checkpoint.engine_response.data_interval;
                    let output_watermark = read_result.checkpoint.engine_response.output_watermark;

                    // New block might not contain anything new, so we check for data
                    // and watermark differences to see if commit should be skipped
                    let metadata_event = if let Some(data_interval) = data_interval {
                        let span = info_span!("Computing data hashes");
                        let _span_guard = span.enter();

                        // TODO: Move out into common data commit procedure of sorts
                        let data_logical_hash =
                            crate::infra::utils::data_utils::get_parquet_logical_hash(&data_path)
                                .map_err(|e| IngestError::internal(e))?;

                        let data_physical_hash =
                            crate::infra::utils::data_utils::get_parquet_physical_hash(&data_path)
                                .map_err(|e| IngestError::internal(e))?;

                        // Advance offset for the next run
                        self.next_offset = data_interval.end + 1;

                        MetadataEvent::AddData(AddData{
                            output_data: DataSlice {
                            logical_hash: data_logical_hash,
                            physical_hash: data_physical_hash,
                            interval: data_interval,
                        },
                        output_watermark,
                    })
                    } else {
                        let prev_watermark = self.meta_chain.borrow()
                            .iter_blocks()
                            .filter_map(|(_, b)| b.into_data_stream_block())
                            .find_map(|b| b.event.output_watermark);

                        if output_watermark.is_none() || output_watermark == prev_watermark {
                            info!("Skipping commit of new block as it neither has new data or watermark");
                            return Ok(ExecutionResult {
                                was_up_to_date: false,  // The checkpoint is not up-to-date but dataset is
                                checkpoint: CommitCheckpoint {
                                    last_committed: Utc::now(),
                                    for_read_at: read_result.checkpoint.last_read,
                                    for_fetched_at: fetch_result.checkpoint.last_fetched,
                                    last_hash: None,
                                },
                            })
                        }
                        MetadataEvent::SetWatermark(SetWatermark{
                            output_watermark: output_watermark.unwrap(),
                        })
                    };

                    let new_block = MetadataBlock {
                        system_time: read_result.checkpoint.system_time,
                        prev_block_hash: Some(prev_hash),
                        event: metadata_event,
                    };

                    let new_head = self.meta_chain.borrow_mut().append(new_block);
                    info!(%new_head, "Committed new block");

                    std::fs::create_dir_all(&self.layout.data_dir).map_err(|e| IngestError::internal(e))?;

                    // TODO: ACID: Data should be moved before writing block file
                    std::fs::rename(
                        &data_path,
                        self.layout.data_dir.join(new_head.to_string()),
                    )
                    .map_err(|e| IngestError::internal(e))?;

                    // TODO: ACID: Checkpoint should be moved before writing block file
                    std::fs::rename(
                        &checkpoint_dir,
                        self.layout.checkpoints_dir.join(new_head.to_string()),
                    )
                    .map_err(|e| IngestError::internal(e))?;

                    Ok(ExecutionResult {
                        was_up_to_date: false,
                        checkpoint: CommitCheckpoint {
                            last_committed: Utc::now(),
                            for_read_at: read_result.checkpoint.last_read,
                            for_fetched_at: fetch_result.checkpoint.last_fetched,
                            last_hash: Some(new_head),
                        },
                    })
                },
            )
            .map_err(|e| IngestError::internal(e))?
    }
}

struct FetchProgressListenerBridge {
    listener: Arc<dyn IngestListener>,
}

impl FetchProgressListener for FetchProgressListenerBridge {
    fn on_progress(&mut self, progress: &FetchProgress) {
        self.listener.on_stage_progress(
            IngestStage::Fetch,
            progress.fetched_bytes,
            progress.total_bytes,
        );
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CommitCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_committed: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_read_at: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_fetched_at: DateTime<Utc>,
    pub last_hash: Option<Multihash>,
}
