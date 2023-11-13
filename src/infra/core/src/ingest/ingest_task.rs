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
use kamu_core::engine::{IngestRequest, IngestResponse};
use kamu_core::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use super::*;

pub struct IngestTask {
    dataset: Arc<dyn Dataset>,
    request: IngestRequest,
    options: PollingIngestOptions,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn PollingIngestListener>,

    prev_source_state: Option<PollingSourceState>,

    fetch_service: FetchService,
    prep_service: PrepService,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    cache_dir: PathBuf,
}

impl IngestTask {
    pub async fn new<'a>(
        dataset: Arc<dyn Dataset>,
        request: IngestRequest,
        options: PollingIngestOptions,
        fetch_override: Option<FetchStep>,
        listener: Arc<dyn PollingIngestListener>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        container_runtime: Arc<ContainerRuntime>,
        run_info_dir: &Path,
        cache_dir: &Path,
    ) -> Result<Self, InternalError> {
        if fetch_override.is_some() {
            if let FetchStep::FilesGlob(_) = &request.polling_source.fetch {
                return Err(concat!(
                    "Fetch override use is not supported for glob sources, ",
                    "as globs maintain strict ordering and state"
                )
                .int_err());
            }
        }

        Ok(Self {
            dataset,
            options,
            fetch_override,
            listener,
            prev_source_state: request
                .prev_source_state
                .as_ref()
                .and_then(|ss| PollingSourceState::try_from_source_state(&ss)),
            fetch_service: FetchService::new(container_runtime, run_info_dir),
            prep_service: PrepService::new(),
            engine_provisioner,
            cache_dir: cache_dir.to_owned(),
            request,
        })
    }

    pub async fn ingest(
        &mut self,
        operation_id: String,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        self.request.operation_id = operation_id;

        tracing::info!(
            request = ?self.request,
            options = ?self.options,
            fetch_override = ?self.fetch_override,
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

    pub async fn ingest_inner(&mut self) -> Result<PollingIngestResult, PollingIngestError> {
        let prev_head = self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        self.listener
            .on_stage_progress(PollingIngestStage::CheckCache, 0, TotalSteps::Exact(1));

        let first_ingest = self.request.next_offset == 0;
        let uncacheable =
            !first_ingest && self.prev_source_state.is_none() && self.fetch_override.is_none();

        if uncacheable && !self.options.fetch_uncacheable {
            tracing::info!("Skipping fetch of uncacheable source");
            return Ok(PollingIngestResult::UpToDate {
                no_source_defined: false,
                uncacheable,
            });
        }

        match self.maybe_fetch().await? {
            FetchStepResult::UpToDate => Ok(PollingIngestResult::UpToDate {
                no_source_defined: false,
                uncacheable,
            }),
            FetchStepResult::Updated(savepoint) => {
                self.listener.on_stage_progress(
                    PollingIngestStage::Prepare,
                    0,
                    TotalSteps::Exact(1),
                );
                let prepare_result = self.prepare(&savepoint).await?;

                self.listener
                    .on_stage_progress(PollingIngestStage::Read, 0, TotalSteps::Exact(1));
                let read_result = self
                    .read(&prepare_result, savepoint.source_event_time)
                    .await?;

                self.listener.on_stage_progress(
                    PollingIngestStage::Preprocess,
                    0,
                    TotalSteps::Exact(1),
                );
                self.listener
                    .on_stage_progress(PollingIngestStage::Merge, 0, TotalSteps::Exact(1));
                self.listener.on_stage_progress(
                    PollingIngestStage::Commit,
                    0,
                    TotalSteps::Exact(1),
                );

                let commit_res = self
                    .maybe_commit(savepoint.source_state.as_ref(), read_result, &prev_head)
                    .await?;

                // Clean up intermediate files
                // Note that we are leaving the fetch data and savepoint intact
                // in case user wants to iterate on the dataset.
                if prepare_result.data != savepoint.data {
                    prepare_result
                        .data
                        .remove_owned(&self.cache_dir)
                        .int_err()?;
                }

                match commit_res {
                    CommitStepResult::UpToDate => Ok(PollingIngestResult::UpToDate {
                        no_source_defined: false,
                        uncacheable,
                    }),
                    CommitStepResult::Updated(commit) => {
                        // Advance the task state for the next run
                        self.request.system_time = Utc::now();
                        self.request.prev_checkpoint =
                            commit.new_event.output_checkpoint.map(|c| c.physical_hash);
                        self.request.prev_watermark = commit.new_event.output_watermark;
                        if let Some(output_data) = commit.new_event.output_data {
                            self.request.next_offset = output_data.interval.end + 1;
                            self.request
                                .prev_data_slices
                                .push(output_data.physical_hash);
                        }
                        self.prev_source_state = savepoint.source_state;
                        self.request.prev_source_state = self
                            .prev_source_state
                            .as_ref()
                            .map(|ss| ss.to_source_state());

                        Ok(PollingIngestResult::Updated {
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

    /// Savepoint is considered valid only when it corresponts to the identical
    /// fetch step and the source state of the previous commit - this way
    /// savepoint is always based on next state increment after the previous
    /// run. We ensure validity by naming the savepoint based on a hash of the
    /// fetch step and the source state in flatbuffers representation.
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
    async fn maybe_fetch(&mut self) -> Result<FetchStepResult, PollingIngestError> {
        // Ignore source state for overridden fetch step
        let (fetch_step, prev_source_state) = if let Some(fetch_override) = &self.fetch_override {
            (fetch_override, None)
        } else {
            (
                &self.request.polling_source.fetch,
                self.prev_source_state.as_ref(),
            )
        };

        let savepoint_path = self.get_savepoint_path(fetch_step, prev_source_state)?;
        let savepoint = self.read_fetch_savepoint(&savepoint_path)?;

        if let Some(savepoint) = savepoint {
            if prev_source_state.is_none() && self.options.fetch_uncacheable {
                tracing::info!(
                    ?savepoint_path,
                    "Ingoring savepoint due to --fetch-uncacheable"
                );
            } else {
                tracing::info!(?savepoint_path, "Resuming from savepoint");
                self.listener.on_cache_hit(&savepoint.created_at);
                return Ok(FetchStepResult::Updated(savepoint));
            }
        }

        // Just in case user deleted it manually
        if !self.cache_dir.exists() {
            std::fs::create_dir(&self.cache_dir).int_err()?;
        }

        let data_cache_key = self.get_random_cache_key("fetch-");
        let target_path = self.cache_dir.join(&data_cache_key);

        let fetch_result = self
            .fetch_service
            .fetch(
                &self.request.operation_id,
                fetch_step,
                prev_source_state,
                &target_path,
                &self.request.system_time,
                Some(Arc::new(FetchProgressListenerBridge::new(
                    self.listener.clone(),
                ))),
            )
            .await?;

        match fetch_result {
            FetchResult::UpToDate => Ok(FetchStepResult::UpToDate),
            FetchResult::Updated(upd) => {
                let data = if let Some(path) = upd.zero_copy_path {
                    SavepointData::Ref { path }
                } else {
                    SavepointData::Owned {
                        cache_key: data_cache_key,
                    }
                };

                let savepoint = FetchSavepoint {
                    created_at: self.request.system_time.clone(),
                    // If fetch source was overridden, we don't want to put its
                    // source state into the metadata.
                    source_state: if self.fetch_override.is_none() {
                        upd.source_state
                    } else {
                        None
                    },
                    source_event_time: upd.source_event_time,
                    data,
                    has_more: upd.has_more,
                };
                self.write_fetch_savepoint(&savepoint_path, &savepoint)?;
                Ok(FetchStepResult::Updated(savepoint))
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn prepare(
        &mut self,
        fetch_result: &FetchSavepoint,
    ) -> Result<PrepStepResult, PollingIngestError> {
        let null_steps = Vec::new();
        let prep_steps = self
            .request
            .polling_source
            .prepare
            .as_ref()
            .unwrap_or(&null_steps);

        if prep_steps.is_empty() {
            // Specify input as output
            Ok(PrepStepResult {
                data: fetch_result.data.clone(),
            })
        } else {
            let src_path = fetch_result.data.path(&self.cache_dir);
            let data_cache_key = self.get_random_cache_key("prepare-");
            let target_path = self.cache_dir.join(&data_cache_key);

            self.prep_service
                .prepare(prep_steps, &src_path, &target_path)?;

            Ok(PrepStepResult {
                data: SavepointData::Owned {
                    cache_key: data_cache_key,
                },
            })
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(
        &mut self,
        prep_result: &PrepStepResult,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<IngestResponse, PollingIngestError> {
        let input_data_path = prep_result.data.path(&self.cache_dir);

        // Terminate early for zero-sized files
        // TODO: Should we still call an engine if only to propagate source_event_time
        // to it?
        if input_data_path.metadata().int_err()?.len() == 0 {
            Ok(IngestResponse {
                data_interval: None,
                output_watermark: self.request.prev_watermark,
                out_checkpoint: None,
                out_data: None,
            })
        } else {
            let engine = self
                .engine_provisioner
                .provision_ingest_engine(self.listener.clone().get_engine_provisioning_listener())
                .await?;

            let response = engine
                .ingest(IngestRequest {
                    event_time: source_event_time,
                    input_data_path,
                    ..self.request.clone()
                })
                .await?;

            Ok(response)
        }
    }

    #[tracing::instrument(level = "info", name = "commit", skip_all)]
    async fn maybe_commit(
        &mut self,
        source_state: Option<&PollingSourceState>,
        read_result: IngestResponse,
        prev_hash: &Multihash,
    ) -> Result<CommitStepResult, PollingIngestError> {
        let data_interval = read_result.data_interval.clone();
        let output_watermark = read_result.output_watermark;
        let source_state = source_state.map(|v| v.to_source_state());

        match self
            .dataset
            .commit_add_data(
                AddDataParams {
                    input_checkpoint: self.request.prev_checkpoint.clone(),
                    output_data: data_interval.clone(),
                    output_watermark,
                    source_state,
                },
                read_result.out_data,
                read_result.out_checkpoint,
                CommitOpts {
                    block_ref: &BlockRef::Head,
                    system_time: Some(self.request.system_time),
                    prev_block_hash: Some(Some(&prev_hash)),
                    check_object_refs: false,
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

pub(crate) enum FetchStepResult {
    UpToDate,
    Updated(FetchSavepoint),
}

pub(crate) struct PrepStepResult {
    pub data: SavepointData,
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

pub(crate) struct FetchProgressListenerBridge {
    listener: Arc<dyn PollingIngestListener>,
}

impl FetchProgressListenerBridge {
    pub(crate) fn new(listener: Arc<dyn PollingIngestListener>) -> Self {
        Self { listener }
    }
}

impl FetchProgressListener for FetchProgressListenerBridge {
    fn on_progress(&self, progress: &FetchProgress) {
        self.listener.on_stage_progress(
            PollingIngestStage::Fetch,
            progress.fetched_bytes,
            match progress.total_bytes {
                TotalBytes::Unknown => TotalSteps::Unknown,
                TotalBytes::Exact(v) => TotalSteps::Exact(v),
            },
        );
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        self.listener.clone().get_pull_image_listener()
    }
}
