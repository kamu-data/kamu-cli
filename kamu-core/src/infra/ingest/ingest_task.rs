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
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use container_runtime::ContainerRuntime;
use std::sync::Arc;
use tracing::{error, info, info_span};

pub struct IngestTask {
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    options: IngestOptions,
    layout: DatasetLayout,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn IngestListener>,

    next_offset: i64,
    source: SetPollingSource,
    prev_checkpoint: Option<Multihash>,
    vocab: DatasetVocabulary,
    checkpointing_executor: CheckpointingExecutor,
    fetch_service: FetchService,
    prep_service: PrepService,
    read_service: ReadService,
}

impl IngestTask {
    pub async fn new<'a>(
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
        options: IngestOptions,
        layout: DatasetLayout,
        fetch_override: Option<FetchStep>,
        listener: Arc<dyn IngestListener>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        container_runtime: Arc<ContainerRuntime>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Result<Self, InternalError> {
        // TODO: PERF: This is expensive and could be cached
        let mut source = None;
        let mut prev_checkpoint = None;
        let mut vocab = None;
        let mut next_offset = 0;

        {
            use futures::stream::TryStreamExt;
            let mut block_stream = dataset.as_metadata_chain().iter_blocks();
            while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                match block.event {
                    MetadataEvent::AddData(add_data) => {
                        if next_offset == 0 {
                            next_offset = add_data.output_data.interval.end + 1;
                        }
                        // TODO: Keep track of other types of blocks that may produce checkpoints
                        if prev_checkpoint.is_none() {
                            prev_checkpoint = add_data.output_checkpoint.map(|cp| cp.physical_hash);
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
                    MetadataEvent::ExecuteQuery(_) => unreachable!(),
                    MetadataEvent::Seed(_)
                    | MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_)
                    | MetadataEvent::SetTransform(_)
                    | MetadataEvent::SetWatermark(_) => (),
                }

                if source.is_some() && vocab.is_some() && prev_checkpoint.is_some() {
                    break;
                }
            }
        }

        let source = source.ok_or_else(|| "Failed to find source definition".int_err())?;

        if fetch_override.is_some() {
            if let FetchStep::FilesGlob(_) = source.fetch {
                return Err(concat!(
                    "Fetch override use is not supported for glob sources, ",
                    "as globs maintain strict ordering and state"
                )
                .int_err());
            }
        }

        Ok(Self {
            options,
            dataset_handle,
            next_offset,
            layout,
            dataset,
            source,
            fetch_override,
            prev_checkpoint,
            vocab: vocab.unwrap_or_default(),
            listener,
            checkpointing_executor: CheckpointingExecutor::new(),
            fetch_service: FetchService::new(container_runtime, workspace_layout),
            prep_service: PrepService::new(),
            read_service: ReadService::new(engine_provisioner),
        })
    }

    pub async fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        let span = info_span!("Ingesting data", dataset_handle = %self.dataset_handle);
        let _span_guard = span.enter();
        info!(source = ?self.source, prev_checkpoint = ?self.prev_checkpoint, vocab = ?self.vocab, "Ingest details");

        self.listener.begin();

        match self.ingest_inner().await {
            Ok(res) => {
                info!(result = ?res, "Ingest successful");
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

    pub async fn ingest_inner(&mut self) -> Result<IngestResult, IngestError> {
        self.listener
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        let prev_hash = self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        let fetch_result = self.maybe_fetch().await?;
        let has_more = fetch_result.checkpoint.has_more;
        let cacheable = fetch_result.checkpoint.is_cacheable();
        let source_event_time = fetch_result.checkpoint.source_event_time.clone();

        self.listener.on_stage_progress(IngestStage::Prepare, 0, 1);

        let prepare_result = self.maybe_prepare(fetch_result.clone()).await?;

        self.listener.on_stage_progress(IngestStage::Read, 0, 1);

        let read_result = self.maybe_read(prepare_result, source_event_time).await?;

        self.listener
            .on_stage_progress(IngestStage::Preprocess, 0, 1);
        self.listener.on_stage_progress(IngestStage::Merge, 0, 1);
        self.listener.on_stage_progress(IngestStage::Commit, 0, 1);

        let commit_result = self
            .maybe_commit(fetch_result, read_result, prev_hash.clone())
            .await?;

        let res = if commit_result.was_up_to_date || commit_result.checkpoint.last_hash.is_none() {
            IngestResult::UpToDate {
                uncacheable: !cacheable,
                has_more,
            }
        } else {
            IngestResult::Updated {
                old_head: prev_hash,
                new_head: commit_result.checkpoint.last_hash.unwrap(),
                num_blocks: 1,
                has_more,
                uncacheable: !cacheable,
            }
        };

        Ok(res)
    }

    async fn maybe_fetch(&mut self) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("fetch.yaml");

        let commit_checkpoint_path = self.layout.cache_dir.join("commit.yaml");
        let commit_checkpoint = self
            .checkpointing_executor
            .read_checkpoint::<CommitCheckpoint>(&commit_checkpoint_path, "CommitCheckpoint")
            .int_err()?;

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                "FetchCheckpoint",
                |old_checkpoint: Option<FetchCheckpoint>| async {
                    // Ingesting from overridden source?
                    if let Some(fetch_override) = &self.fetch_override {
                        info!(fetch = ?fetch_override, "Fetching the overridden source");

                        return self
                            .fetch_service
                            .fetch(
                                fetch_override,
                                None,
                                &self.layout.cache_dir.join("fetched.bin"),
                                Some(Arc::new(FetchProgressListenerBridge {
                                    listener: self.listener.clone(),
                                })),
                            )
                            .await
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
                        if !cp.is_cacheable() && !self.options.fetch_uncacheable {
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

                    self.fetch_service
                        .fetch(
                            &self.source.fetch,
                            old_checkpoint,
                            &self.layout.cache_dir.join("fetched.bin"),
                            Some(Arc::new(FetchProgressListenerBridge {
                                listener: self.listener.clone(),
                            })),
                        )
                        .await
                },
            )
            .await
            .int_err()?
    }

    // TODO: PERF: Skip the copying if there are no prep steps
    async fn maybe_prepare(
        &mut self,
        fetch_result: ExecutionResult<FetchCheckpoint>,
    ) -> Result<ExecutionResult<PrepCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("prep.yaml");

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                "PrepCheckpoint",
                |old_checkpoint: Option<PrepCheckpoint>| async {
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
            .await
            .int_err()?
    }

    async fn maybe_read(
        &mut self,
        prep_result: ExecutionResult<PrepCheckpoint>,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<ExecutionResult<ReadCheckpoint>, IngestError> {
        let checkpoint_path = self.layout.cache_dir.join("read.yaml");

        self.checkpointing_executor
            .execute(
                &checkpoint_path,
                "ReadCheckpoint",
                |old_checkpoint: Option<ReadCheckpoint>| async {
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

                    self.read_service
                        .read(
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
                        .await
                },
            )
            .await
            .int_err()?
    }

    // TODO: Atomicity
    async fn maybe_commit(
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
                |old_checkpoint: Option<CommitCheckpoint>| async {
                    if let Some(ref cp) = old_checkpoint {
                        if cp.for_read_at == read_result.checkpoint.last_read {
                            return Ok(ExecutionResult {
                                was_up_to_date: true,
                                checkpoint: old_checkpoint.unwrap(),
                            });
                        }
                    }

                    let cache_files = vec![
                        self.layout.cache_dir.join("fetched.bin"),
                        self.layout.cache_dir.join("prepared.bin"),
                    ];
                    let data_interval = read_result.checkpoint.engine_response.data_interval;
                    let output_watermark = read_result.checkpoint.engine_response.output_watermark;
                    let new_data_path = if data_interval.is_some() {
                        Some(self.layout.cache_dir.join("read.bin"))
                    } else {
                        None
                    };
                    let new_checkpoint_path = self.layout.cache_dir.join("read.checkpoint");
                    let new_checkpoint_path = if new_checkpoint_path.exists() {
                        Some(new_checkpoint_path)
                    } else {
                        None
                    };

                    match self
                        .dataset
                        .commit_add_data(
                            self.prev_checkpoint.clone(),
                            data_interval.clone(),
                            new_data_path,
                            new_checkpoint_path,
                            output_watermark,
                            CommitOpts {
                                block_ref: &BlockRef::Head,
                                system_time: Some(read_result.checkpoint.system_time),
                                prev_block_hash: Some(Some(&prev_hash)),
                            },
                        )
                        .await
                    {
                        Ok(commit_result) => {
                            // Advance offset for the next run
                            if let Some(data_interval) = data_interval {
                                self.next_offset = data_interval.end + 1;
                            }

                            // Clean up intermediate files
                            for path in cache_files {
                                std::fs::remove_file(path).int_err()?;
                            }

                            Ok(ExecutionResult {
                                was_up_to_date: false,
                                checkpoint: CommitCheckpoint {
                                    last_committed: Utc::now(),
                                    for_read_at: read_result.checkpoint.last_read,
                                    for_fetched_at: fetch_result.checkpoint.last_fetched,
                                    last_hash: Some(commit_result.new_head),
                                },
                            })
                        }
                        Err(CommitError::EmptyCommit) => {
                            Ok(ExecutionResult {
                                was_up_to_date: false, // The checkpoint is not up-to-date but dataset is
                                checkpoint: CommitCheckpoint {
                                    last_committed: Utc::now(),
                                    for_read_at: read_result.checkpoint.last_read,
                                    for_fetched_at: fetch_result.checkpoint.last_fetched,
                                    last_hash: None,
                                },
                            })
                        }
                        Err(e) => Err(e.int_err().into()),
                    }
                },
            )
            .await
            .int_err()?
    }
}

struct FetchProgressListenerBridge {
    listener: Arc<dyn IngestListener>,
}

impl FetchProgressListener for FetchProgressListenerBridge {
    fn on_progress(&self, progress: &FetchProgress) {
        self.listener.on_stage_progress(
            IngestStage::Fetch,
            progress.fetched_bytes,
            progress.total_bytes,
        );
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        self.listener.clone().get_pull_image_listener()
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
    // Contains either last block's hash or None if no data was produced by last ingest
    pub last_hash: Option<Multihash>,
}
