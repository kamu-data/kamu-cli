// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use container_runtime::ContainerRuntime;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use super::*;
use crate::domain::*;
use crate::infra::*;

pub struct IngestTask {
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    options: IngestOptions,
    layout: DatasetLayout,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn IngestListener>,

    next_offset: i64,
    source: Option<SetPollingSource>,
    prev_source_state: Option<PollingSourceState>,
    prev_checkpoint: Option<Multihash>,
    prev_watermark: Option<DateTime<Utc>>,
    vocab: DatasetVocabulary,

    fetch_service: FetchService,
    prep_service: PrepService,
    read_service: ReadService,
    cache_dir: PathBuf,
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
        let mut prev_source_state = None;
        let mut prev_checkpoint = None;
        let mut prev_watermark = None;
        let mut vocab = None;
        let mut next_offset = None;

        {
            use futures::stream::TryStreamExt;
            let mut block_stream = dataset.as_metadata_chain().iter_blocks();
            while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                match block.event {
                    MetadataEvent::AddData(add_data) => {
                        if next_offset.is_none() {
                            if let Some(output_data) = &add_data.output_data {
                                next_offset = Some(output_data.interval.end + 1);
                            }
                        }
                        if prev_checkpoint.is_none() {
                            prev_checkpoint =
                                Some(add_data.output_checkpoint.map(|cp| cp.physical_hash));
                        }
                        if prev_watermark.is_none() {
                            prev_watermark = Some(add_data.output_watermark);
                        }
                        if prev_source_state.is_none() {
                            if let Some(source_state) = add_data.source_state {
                                prev_source_state =
                                    PollingSourceState::from_source_state(&source_state)?;
                            }
                        }
                    }
                    MetadataEvent::SetWatermark(set_wm) => {
                        if prev_watermark.is_none() {
                            prev_watermark = Some(Some(set_wm.output_watermark));
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
                    MetadataEvent::Seed(_) => {
                        if next_offset.is_none() {
                            next_offset = Some(0);
                        }
                    }
                    MetadataEvent::ExecuteQuery(_) => unreachable!(),
                    MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_)
                    | MetadataEvent::SetTransform(_) => (),
                }

                if next_offset.is_some()
                    && source.is_some()
                    && vocab.is_some()
                    && prev_checkpoint.is_some()
                    && prev_watermark.is_some()
                {
                    break;
                }
            }
        }

        if let Some(source) = &source {
            if fetch_override.is_some() {
                if let FetchStep::FilesGlob(_) = &source.fetch {
                    return Err(concat!(
                        "Fetch override use is not supported for glob sources, ",
                        "as globs maintain strict ordering and state"
                    )
                    .int_err());
                }
            }
        }

        Ok(Self {
            options,
            dataset_handle,
            next_offset: next_offset.unwrap(),
            layout,
            dataset,
            fetch_override,
            listener,
            source,
            prev_source_state,
            prev_checkpoint: prev_checkpoint.unwrap_or_default(),
            prev_watermark: prev_watermark.unwrap_or_default(),
            vocab: vocab.unwrap_or_default(),
            fetch_service: FetchService::new(container_runtime, &workspace_layout.run_info_dir),
            prep_service: PrepService::new(),
            read_service: ReadService::new(engine_provisioner),
            cache_dir: workspace_layout.cache_dir.clone(),
        })
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(dataset_handle = %self.dataset_handle),
    )]
    pub async fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        tracing::info!(
            source = ?self.source,
            fetch_override = ?self.fetch_override,
            prev_checkpoint = ?self.prev_checkpoint,
            vocab = ?self.vocab,
            "Ingest details",
        );

        self.listener.begin();

        match self.ingest_inner().await {
            Ok(res) => {
                tracing::info!(result = ?res, "Ingest successful");
                self.listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, "Ingest failed");
                self.listener.error(&err);
                Err(err)
            }
        }
    }

    pub async fn ingest_inner(&mut self) -> Result<IngestResult, IngestError> {
        if self.source.is_none() {
            return Ok(IngestResult::UpToDate {
                no_polling_source: true,
                uncacheable: false,
            });
        }

        let prev_head = self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        self.listener
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        let first_ingest = self.next_offset == 0;
        let uncacheable =
            !first_ingest && self.prev_source_state.is_none() && self.fetch_override.is_none();

        if uncacheable && !self.options.fetch_uncacheable {
            tracing::info!("Skipping fetch of uncacheable source");
            return Ok(IngestResult::UpToDate {
                no_polling_source: false,
                uncacheable,
            });
        }

        match self.maybe_fetch().await? {
            FetchStepResult::UpToDate => Ok(IngestResult::UpToDate {
                no_polling_source: false,
                uncacheable,
            }),
            FetchStepResult::Updated(savepoint) => {
                self.listener.on_stage_progress(IngestStage::Prepare, 0, 1);
                let prepare_result = self.prepare(&savepoint).await?;

                self.listener.on_stage_progress(IngestStage::Read, 0, 1);
                let read_result = self
                    .read(&prepare_result, savepoint.source_event_time)
                    .await?;

                self.listener
                    .on_stage_progress(IngestStage::Preprocess, 0, 1);
                self.listener.on_stage_progress(IngestStage::Merge, 0, 1);
                self.listener.on_stage_progress(IngestStage::Commit, 0, 1);

                let commit_res = self
                    .maybe_commit(savepoint.source_state.as_ref(), &read_result, &prev_head)
                    .await?;

                // Clean up intermediate files
                // Note that we are leaving the fetch data and savepoint intact
                // in case user wants to iterate on the dataset.
                if prepare_result.data_cache_key != savepoint.data_cache_key {
                    std::fs::remove_file(self.cache_dir.join(prepare_result.data_cache_key))
                        .int_err()?;
                }

                match commit_res {
                    CommitStepResult::UpToDate => Ok(IngestResult::UpToDate {
                        no_polling_source: false,
                        uncacheable,
                    }),
                    CommitStepResult::Updated(commit) => {
                        // Advance the task state for the next run
                        self.prev_checkpoint =
                            commit.new_event.output_checkpoint.map(|c| c.physical_hash);
                        self.prev_watermark = commit.new_event.output_watermark;
                        if let Some(output_data) = commit.new_event.output_data {
                            self.next_offset = output_data.interval.end + 1;
                        }
                        self.prev_source_state = savepoint.source_state;

                        Ok(IngestResult::Updated {
                            old_head: prev_head,
                            new_head: commit.new_head,
                            num_blocks: 1,
                            has_more: savepoint.has_more,
                            uncacheable,
                        })
                    }
                }
            }
        }
    }

    fn get_random_cache_key(&self, prefix: &str) -> String {
        use rand::distributions::Alphanumeric;
        use rand::Rng;

        let mut name = String::with_capacity(10 + prefix.len());
        name.push_str(prefix);
        name.extend(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from),
        );
        name
    }

    /// Savepoint is considered valid only when it corresponts to the identical fetch step and
    /// the source state of the previous commit - this way savepoint is always based on next state increment
    /// after the previous run. We ensure validity by naming the savepoint based on a hash of the fetch step
    /// and the source state in flatbuffers representation.
    fn get_savepoint_path(
        &self,
        fetch_step: &FetchStep,
        source_state: Option<&PollingSourceState>,
    ) -> Result<PathBuf, InternalError> {
        use opendatafabric::serde::flatbuffers::{
            FlatbuffersEnumSerializable,
            FlatbuffersSerializable,
        };

        let mut fb = ::flatbuffers::FlatBufferBuilder::with_capacity(1024);
        let (_type, offset) = fetch_step.serialize(&mut fb);

        if let Some(source_state) = source_state {
            let source_state = source_state.to_source_state();
            let offset = source_state.serialize(&mut fb);
            fb.finish(offset, None);
        } else {
            fb.finish(offset, None);
        }

        let hash = Multihash::from_digest_sha3_256(fb.finished_data());

        Ok(self
            .cache_dir
            .join(format!("fetch-savepoint-{}", hash.to_multibase_string())))
    }

    fn read_fetch_savepoint(
        &self,
        savepoint_path: &Path,
    ) -> Result<Option<FetchSavepoint>, InternalError> {
        if !savepoint_path.is_file() {
            return Ok(None);
        }

        let manifest: Manifest<FetchSavepoint> =
            serde_yaml::from_reader(std::fs::File::open(savepoint_path).int_err()?).int_err()?;

        Ok(Some(manifest.content))
    }

    fn write_fetch_savepoint(
        &self,
        savepoint_path: &Path,
        savepoint: &FetchSavepoint,
    ) -> Result<(), InternalError> {
        let manifest = Manifest {
            kind: "FetchSavepoint".to_owned(),
            version: 1,
            content: savepoint.clone(),
        };

        serde_yaml::to_writer(std::fs::File::create(savepoint_path).int_err()?, &manifest)
            .int_err()?;

        Ok(())
    }

    #[tracing::instrument(level = "info", name = "fetch", skip(self))]
    async fn maybe_fetch(&mut self) -> Result<FetchStepResult, IngestError> {
        // Ignore source state for overridden fetch step
        let (fetch_step, prev_source_state) = if let Some(fetch_override) = &self.fetch_override {
            (fetch_override, None)
        } else {
            (
                &self.source.as_ref().unwrap().fetch,
                self.prev_source_state.as_ref(),
            )
        };

        let savepoint_path = self.get_savepoint_path(fetch_step, prev_source_state)?;
        let savepoint = self.read_fetch_savepoint(&savepoint_path)?;

        if let Some(savepoint) = savepoint {
            tracing::info!(?savepoint_path, "Resuming from savepoint");
            Ok(FetchStepResult::Updated(savepoint))
        } else {
            // Just in case user deleted it manually
            if !self.cache_dir.exists() {
                std::fs::create_dir(&self.cache_dir).int_err()?;
            }

            let data_cache_key = self.get_random_cache_key("fetch-");
            let target_path = self.cache_dir.join(&data_cache_key);

            let fetch_result = self
                .fetch_service
                .fetch(
                    fetch_step,
                    prev_source_state,
                    &target_path,
                    Some(Arc::new(FetchProgressListenerBridge {
                        listener: self.listener.clone(),
                    })),
                )
                .await?;

            match fetch_result {
                FetchResult::UpToDate => Ok(FetchStepResult::UpToDate),
                FetchResult::Updated(upd) => {
                    let savepoint = FetchSavepoint {
                        // If fetch source was overridden we don't want to put its
                        // source state into the metadata.
                        source_state: if self.fetch_override.is_none() {
                            upd.source_state
                        } else {
                            None
                        },
                        source_event_time: upd.source_event_time,
                        data_cache_key,
                        has_more: upd.has_more,
                    };
                    self.write_fetch_savepoint(&savepoint_path, &savepoint)?;
                    Ok(FetchStepResult::Updated(savepoint))
                }
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn prepare(
        &mut self,
        fetch_result: &FetchSavepoint,
    ) -> Result<PrepStepResult, IngestError> {
        let null_steps = Vec::new();
        let prep_steps = self
            .source
            .as_ref()
            .unwrap()
            .prepare
            .as_ref()
            .unwrap_or(&null_steps);

        if prep_steps.is_empty() {
            Ok(PrepStepResult {
                // Specify input as output
                data_cache_key: fetch_result.data_cache_key.clone(),
            })
        } else {
            let src_path = self.cache_dir.join(&fetch_result.data_cache_key);
            let data_cache_key = self.get_random_cache_key("prepare-");
            let target_path = self.cache_dir.join(&data_cache_key);

            self.prep_service
                .prepare(prep_steps, &src_path, &target_path)?;

            Ok(PrepStepResult { data_cache_key })
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(
        &mut self,
        prep_result: &PrepStepResult,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<ReadStepResult, IngestError> {
        let system_time = Utc::now(); // TODO: Inject

        let src_data_path = self.cache_dir.join(&prep_result.data_cache_key);

        let out_data_cache_key = self.get_random_cache_key("read-");
        let out_data_path = self.cache_dir.join(&out_data_cache_key);

        let out_checkpoint_cache_key = self.get_random_cache_key("read-cpt-");
        let out_checkpoint_path = self.cache_dir.join(&out_checkpoint_cache_key);

        let engine_response = self
            .read_service
            .read(
                &self.dataset_handle,
                &self.layout,
                self.source.as_ref().unwrap(),
                &src_data_path,
                self.prev_watermark.clone(),
                self.prev_checkpoint.clone(),
                &self.vocab,
                system_time.clone(),
                source_event_time,
                self.next_offset,
                &out_data_path,
                &out_checkpoint_path,
                self.listener.clone(),
            )
            .await?;

        Ok(ReadStepResult {
            system_time,
            engine_response,
            out_checkpoint_cache_key,
            out_data_cache_key,
        })
    }

    #[tracing::instrument(level = "info", name = "commit", skip_all)]
    async fn maybe_commit(
        &mut self,
        source_state: Option<&PollingSourceState>,
        read_result: &ReadStepResult,
        prev_hash: &Multihash,
    ) -> Result<CommitStepResult, IngestError> {
        let data_interval = read_result.engine_response.data_interval.clone();
        let output_watermark = read_result.engine_response.output_watermark;
        let new_data_path = if data_interval.is_some() {
            Some(self.cache_dir.join(&read_result.out_data_cache_key))
        } else {
            None
        };
        let new_checkpoint_path = self.cache_dir.join(&read_result.out_checkpoint_cache_key);
        let new_checkpoint_path = if new_checkpoint_path.exists() {
            Some(new_checkpoint_path)
        } else {
            None
        };
        let source_state = source_state.map(|v| v.to_source_state());

        match self
            .dataset
            .commit_add_data(
                self.prev_checkpoint.clone(),
                data_interval.clone(),
                new_data_path,
                new_checkpoint_path,
                output_watermark,
                source_state,
                CommitOpts {
                    block_ref: &BlockRef::Head,
                    system_time: Some(read_result.system_time),
                    prev_block_hash: Some(Some(&prev_hash)),
                },
            )
            .await
        {
            Ok(commit) => {
                let new_block = self
                    .dataset
                    .as_metadata_chain()
                    .get_block(&commit.new_head)
                    .await
                    .int_err()?;

                Ok(CommitStepResult::Updated(CommitStepResultUpdated {
                    new_head: commit.new_head,
                    new_event: match new_block.event {
                        MetadataEvent::AddData(v) => v,
                        _ => unreachable!(),
                    },
                }))
            }
            Err(CommitError::MetadataAppendError(AppendError::InvalidBlock(
                AppendValidationError::NoOpEvent(_),
            ))) => Ok(CommitStepResult::UpToDate),
            Err(err) => Err(err.int_err().into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

enum FetchStepResult {
    UpToDate,
    Updated(FetchSavepoint),
}

struct PrepStepResult {
    data_cache_key: String,
}

struct ReadStepResult {
    system_time: DateTime<Utc>,
    engine_response: ExecuteQueryResponseSuccess,
    out_checkpoint_cache_key: String,
    out_data_cache_key: String,
}

enum CommitStepResult {
    UpToDate,
    Updated(CommitStepResultUpdated),
}

struct CommitStepResultUpdated {
    new_head: Multihash,
    new_event: AddData,
}

///////////////////////////////////////////////////////////////////////////////

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
