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
use kamu_core::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use super::*;

pub struct IngestTask {
    dataset: Arc<dyn Dataset>,
    request: IngestRequest,
    options: IngestOptions,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn IngestListener>,

    prev_source_state: Option<PollingSourceState>,

    fetch_service: FetchService,
    prep_service: PrepService,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    run_info_dir: PathBuf,
    cache_dir: PathBuf,
}

impl IngestTask {
    pub async fn new<'a>(
        dataset: Arc<dyn Dataset>,
        request: IngestRequest,
        options: IngestOptions,
        fetch_override: Option<FetchStep>,
        listener: Arc<dyn IngestListener>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        container_runtime: Arc<ContainerRuntime>,
        run_info_dir: &Path,
        cache_dir: &Path,
    ) -> Result<Self, InternalError> {
        if let Some(polling_source) = &request.polling_source {
            if fetch_override.is_some() {
                if let FetchStep::FilesGlob(_) = &polling_source.fetch {
                    return Err(concat!(
                        "Fetch override use is not supported for glob sources, ",
                        "as globs maintain strict ordering and state"
                    )
                    .int_err());
                }
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
            fetch_service: FetchService::new(container_runtime, &run_info_dir),
            prep_service: PrepService::new(),
            engine_provisioner,
            object_store_registry,
            run_info_dir: run_info_dir.to_owned(),
            cache_dir: cache_dir.to_owned(),
            request,
        })
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            operation_id = %self.request.operation_id,
            dataset_handle = %self.request.dataset_handle,
        ),
    )]
    pub async fn ingest(&mut self) -> Result<IngestResult, IngestError> {
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

    pub async fn ingest_inner(&mut self) -> Result<IngestResult, IngestError> {
        if self.request.polling_source.is_none() {
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
            .on_stage_progress(IngestStage::CheckCache, 0, TotalSteps::Exact(1));

        let first_ingest = self.request.next_offset == 0;
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
                self.listener
                    .on_stage_progress(IngestStage::Prepare, 0, TotalSteps::Exact(1));
                let prepare_result = self.prepare(&savepoint).await?;

                self.listener
                    .on_stage_progress(IngestStage::Read, 0, TotalSteps::Exact(1));
                let read_result = self
                    .read(&prepare_result, savepoint.source_event_time)
                    .await?;

                self.listener
                    .on_stage_progress(IngestStage::Preprocess, 0, TotalSteps::Exact(1));
                self.listener
                    .on_stage_progress(IngestStage::Merge, 0, TotalSteps::Exact(1));
                self.listener
                    .on_stage_progress(IngestStage::Commit, 0, TotalSteps::Exact(1));

                let commit_res = self
                    .maybe_commit(savepoint.source_state.as_ref(), read_result, &prev_head)
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
                        self.request.system_time = Utc::now();
                        self.request.prev_checkpoint =
                            commit.new_event.output_checkpoint.map(|c| c.physical_hash);
                        self.request.prev_watermark = commit.new_event.output_watermark;
                        if let Some(output_data) = commit.new_event.output_data {
                            self.request.next_offset = output_data.interval.end + 1;
                        }
                        self.prev_source_state = savepoint.source_state;
                        self.request.prev_source_state = self
                            .prev_source_state
                            .as_ref()
                            .map(|ss| ss.to_source_state());

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
    async fn maybe_fetch(&mut self) -> Result<FetchStepResult, IngestError> {
        // Ignore source state for overridden fetch step
        let (fetch_step, prev_source_state) = if let Some(fetch_override) = &self.fetch_override {
            (fetch_override, None)
        } else {
            (
                &self.request.polling_source.as_ref().unwrap().fetch,
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
                    &self.request.operation_id,
                    fetch_step,
                    prev_source_state,
                    &target_path,
                    &self.request.system_time,
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
            .request
            .polling_source
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
    ) -> Result<IngestResponse, IngestError> {
        let input_data_path = self.cache_dir.join(&prep_result.data_cache_key);

        // Terminate early for zero-sized files
        // TODO: Should we still call an engine if only to propagate source_event_time
        // to it?
        if input_data_path.metadata().int_err()?.len() == 0 {
            return Ok(IngestResponse {
                data_interval: None,
                output_watermark: self.request.prev_watermark,
                out_checkpoint: None,
                out_data: None,
            });
        }

        let request = IngestRequest {
            event_time: source_event_time,
            input_data_path,
            ..self.request.clone()
        };

        // TODO: Temporarily we continue to default to Spark ingest, but use new engine
        // if preprocessing query specifies datafusion
        let engine = self
            .request
            .polling_source
            .as_ref()
            .unwrap()
            .preprocess
            .as_ref()
            .map(|Transform::Sql(t)| t.engine.as_str());

        let read_svc: Box<dyn ReadService> = if engine == Some("datafusion") {
            Box::new(ReadServiceDatafusion::new(
                self.object_store_registry.clone(),
                self.dataset.clone(),
                self.run_info_dir.clone(),
            ))
        } else {
            Box::new(ReadServiceSpark::new(
                self.engine_provisioner.clone(),
                self.listener.clone(),
            ))
        };
        read_svc.read(request).await
    }

    #[tracing::instrument(level = "info", name = "commit", skip_all)]
    async fn maybe_commit(
        &mut self,
        source_state: Option<&PollingSourceState>,
        read_result: IngestResponse,
        prev_hash: &Multihash,
    ) -> Result<CommitStepResult, IngestError> {
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

enum FetchStepResult {
    UpToDate,
    Updated(FetchSavepoint),
}

struct PrepStepResult {
    data_cache_key: String,
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
