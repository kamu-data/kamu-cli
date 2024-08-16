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
use datafusion::prelude::{DataFrame, SessionContext};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::ingest::*;
use kamu_core::*;
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;
use random_names::get_random_name;
use time_source::SystemTimeSource;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PollingIngestServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    fetch_service: Arc<FetchService>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    data_format_registry: Arc<dyn DataFormatRegistry>,
    run_info_dir: Arc<RunInfoDir>,
    cache_dir: Arc<CacheDir>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn PollingIngestService)]
impl PollingIngestServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        fetch_service: Arc<FetchService>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        data_format_registry: Arc<dyn DataFormatRegistry>,
        run_info_dir: Arc<RunInfoDir>,
        cache_dir: Arc<CacheDir>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            fetch_service,
            engine_provisioner,
            object_store_registry,
            data_format_registry,
            run_info_dir,
            cache_dir,
            time_source,
        }
    }

    async fn do_ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: PollingIngestOptions,
        get_listener: impl FnOnce(&DatasetHandle) -> Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Write)
            .await?;

        let dataset = self.dataset_repo.get_dataset_by_handle(&dataset_handle);

        let listener =
            get_listener(&dataset_handle).unwrap_or_else(|| Arc::new(NullPollingIngestListener));

        self.ingest_loop(IngestLoopArgs {
            dataset_handle,
            dataset,
            options,
            listener,
        })
        .await
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            dataset_handle = %args.dataset_handle,
        )
    )]
    async fn ingest_loop(
        &self,
        args: IngestLoopArgs,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        let ctx = ingest_common::new_session_context(self.object_store_registry.clone());
        let mut data_writer = DataWriterDataFusion::builder(args.dataset.clone(), ctx.clone())
            .with_metadata_state_scanned(None)
            .await
            .int_err()?
            .build();

        let Some(MetadataEvent::SetPollingSource(polling_source)) =
            data_writer.source_event().cloned()
        else {
            tracing::warn!("Dataset does not define a polling source - considering up-to-date",);

            let result = PollingIngestResult::UpToDate {
                no_source_defined: true,
                uncacheable: false,
            };

            args.listener.begin();
            args.listener.success(&result);
            return Ok(result);
        };

        let mut iteration = 0;
        let mut combined_result = None;
        loop {
            iteration += 1;
            let operation_id = get_random_name(None, 10);

            let operation_dir = self.run_info_dir.join(format!("ingest-{operation_id}"));
            std::fs::create_dir_all(&operation_dir).int_err()?;

            let new_ctx = ingest_common::new_session_context(self.object_store_registry.clone());
            data_writer.set_session_context(new_ctx.clone());

            // TODO: Avoid excessive cloning
            let iteration_args = IngestIterationArgs {
                dataset_handle: args.dataset_handle.clone(),
                iteration,
                operation_id,
                operation_dir,
                system_time: self.time_source.now(),
                options: args.options.clone(),
                polling_source: polling_source.clone(),
                listener: args.listener.clone(),
                ctx: new_ctx,
                data_writer: &mut data_writer,
            };

            match self.ingest_iteration(iteration_args).await {
                Ok(res) => {
                    combined_result = Some(Self::merge_results(combined_result, res));

                    let has_more = match combined_result {
                        Some(PollingIngestResult::UpToDate { .. }) => false,
                        Some(PollingIngestResult::Updated { has_more, .. }) => has_more,
                        None => unreachable!(),
                    };

                    if !has_more || !args.options.exhaust_sources {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(combined_result.unwrap())
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            iteration = %args.iteration,
            operation_id = %args.operation_id,
        )
    )]
    async fn ingest_iteration(
        &self,
        args: IngestIterationArgs<'_>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        tracing::info!(
            options = ?args.options,
            "Ingest iteration details",
        );

        let listener = args.listener.clone();
        listener.begin();

        match self.ingest_iteration_inner(args).await {
            Ok(res) => {
                tracing::info!(result = ?res, "Ingest iteration successful");
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, "Ingest iteration failed");
                listener.error(&err);
                Err(err)
            }
        }
    }

    async fn ingest_iteration_inner(
        &self,
        args: IngestIterationArgs<'_>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        args.listener
            .on_stage_progress(PollingIngestStage::CheckCache, 0, TotalSteps::Exact(1));

        let uncacheable = args.data_writer.prev_offset().is_some()
            && args.data_writer.prev_source_state().is_none()
            && !matches!(args.polling_source.fetch, FetchStep::Mqtt(_));

        if uncacheable && !args.options.fetch_uncacheable {
            tracing::info!("Skipping fetch of uncacheable source");
            return Ok(PollingIngestResult::UpToDate {
                no_source_defined: false,
                uncacheable,
            });
        }

        let savepoint = match self.fetch(&args).await? {
            FetchStepResult::Updated(savepoint) => savepoint,
            FetchStepResult::UpToDate => {
                return Ok(PollingIngestResult::UpToDate {
                    no_source_defined: false,
                    uncacheable,
                })
            }
        };

        args.listener
            .on_stage_progress(PollingIngestStage::Prepare, 0, TotalSteps::Exact(1));

        let prepare_result = self
            .prepare(&args, &savepoint, self.run_info_dir.clone())
            .await?;

        args.listener
            .on_stage_progress(PollingIngestStage::Read, 0, TotalSteps::Exact(1));

        let df = if let Some(df) = self.read(&args, &prepare_result).await? {
            tracing::info!(schema = ?df.schema(), "Read the input data frame");

            if let Some(transform) = &args.polling_source.preprocess {
                args.listener.on_stage_progress(
                    PollingIngestStage::Preprocess,
                    0,
                    TotalSteps::Exact(1),
                );

                ingest_common::preprocess(
                    &args.operation_id,
                    self.engine_provisioner.as_ref(),
                    &args.ctx,
                    transform,
                    df,
                    args.listener.clone().get_engine_provisioning_listener(),
                )
                .await?
            } else {
                Some(df)
            }
        } else {
            tracing::info!("Read produced an empty data frame");
            None
        };

        let new_source_state = savepoint.source_state.map(|ss| ss.to_source_state());

        let out_dir = args.operation_dir.join("out");
        let data_staging_path = out_dir.join("data");
        std::fs::create_dir(&out_dir).int_err()?;

        let stage_result = args
            .data_writer
            .stage(
                df,
                WriteDataOpts {
                    system_time: args.system_time,
                    source_event_time: savepoint.source_event_time.unwrap_or(args.system_time),
                    new_watermark: None,
                    new_source_state,
                    data_staging_path,
                },
            )
            .await;

        // Clean up intermediate files
        // Note that we are leaving the fetch data and savepoint intact
        // in case user wants to iterate on the dataset.
        if prepare_result.data != savepoint.data {
            prepare_result
                .data
                .remove_owned(&self.cache_dir)
                .int_err()?;
        }

        match stage_result {
            Ok(staged) => {
                args.listener.on_stage_progress(
                    PollingIngestStage::Commit,
                    0,
                    TotalSteps::Exact(1),
                );

                let res = args.data_writer.commit(staged).await?;

                Ok(PollingIngestResult::Updated {
                    old_head: res.old_head,
                    new_head: res.new_head,
                    has_more: savepoint.has_more,
                    uncacheable,
                })
            }
            Err(StageDataError::BadInputSchema(e)) => Err(e.into()),
            Err(StageDataError::IncompatibleSchema(e)) => Err(e.into()),
            Err(StageDataError::MergeError(e)) => Err(e.into()),
            Err(StageDataError::EmptyCommit(_)) => Ok(PollingIngestResult::UpToDate {
                no_source_defined: false,
                uncacheable,
            }),
            Err(StageDataError::Internal(e)) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn fetch(
        &self,
        args: &IngestIterationArgs<'_>,
    ) -> Result<FetchStepResult, PollingIngestError> {
        let fetch_step = &args.polling_source.fetch;
        let prev_source_state = args
            .data_writer
            .prev_source_state()
            .and_then(PollingSourceState::try_from_source_state);

        let savepoint_path = self.get_savepoint_path(fetch_step, prev_source_state.as_ref());
        let savepoint = self.read_fetch_savepoint(&savepoint_path)?;

        if let Some(savepoint) = savepoint {
            if prev_source_state.is_none() && args.options.fetch_uncacheable {
                tracing::info!(
                    ?savepoint_path,
                    "Ignoring savepoint due to --fetch-uncacheable"
                );
            } else if let FetchStep::Mqtt(_) = fetch_step {
            } else {
                tracing::info!(?savepoint_path, "Resuming from savepoint");
                args.listener.on_cache_hit(&savepoint.created_at);
                return Ok(FetchStepResult::Updated(savepoint));
            }
        }

        // Just in case user deleted it manually
        if !self.cache_dir.exists() {
            std::fs::create_dir(self.cache_dir.as_path()).int_err()?;
        }

        let data_cache_key = get_random_name(Some("fetch-"), 10);
        let target_path = self.cache_dir.join(&data_cache_key);

        let fetch_result = self
            .fetch_service
            .fetch(
                &args.dataset_handle,
                &args.operation_id,
                fetch_step,
                prev_source_state.as_ref(),
                &target_path,
                &args.system_time,
                &args.options.dataset_env_vars,
                Some(Arc::new(FetchProgressListenerBridge::new(
                    args.listener.clone(),
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
                    created_at: args.system_time,
                    source_state: upd.source_state,
                    source_event_time: upd.source_event_time,
                    data,
                    has_more: upd.has_more,
                };
                self.write_fetch_savepoint(&savepoint_path, &savepoint)?;
                Ok(FetchStepResult::Updated(savepoint))
            }
        }
    }

    /// Savepoint is considered valid only when it corresponds to the identical
    /// fetch step and the source state of the previous commit - this way
    /// savepoint is always based on next state increment after the previous
    /// run. We ensure validity by naming the savepoint based on a hash of the
    /// fetch step and the source state in flatbuffers representation.
    fn get_savepoint_path(
        &self,
        fetch_step: &FetchStep,
        source_state: Option<&PollingSourceState>,
    ) -> PathBuf {
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

        self.cache_dir.join(format!("fetch-savepoint-{hash}"))
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn prepare(
        &self,
        args: &IngestIterationArgs<'_>,
        fetch_result: &FetchSavepoint,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Result<PrepStepResult, PollingIngestError> {
        let prep_steps = args.polling_source.prepare.clone().unwrap_or_default();

        if prep_steps.is_empty() {
            // Specify input as output
            Ok(PrepStepResult {
                data: fetch_result.data.clone(),
            })
        } else {
            let src_path = fetch_result.data.path(&self.cache_dir);
            let data_cache_key = get_random_name(Some("prepare-"), 10);
            let target_path = self.cache_dir.join(&data_cache_key);

            tracing::debug!(
                ?src_path,
                ?target_path,
                ?prep_steps,
                "Executing prepare step",
            );

            // TODO: Make async
            tokio::task::spawn_blocking(move || {
                let prep_service = PrepService::new();
                prep_service.prepare(&prep_steps, &src_path, &target_path, &run_info_dir)
            })
            .await
            .int_err()??;

            Ok(PrepStepResult {
                data: SavepointData::Owned {
                    cache_key: data_cache_key,
                },
            })
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(
        &self,
        args: &IngestIterationArgs<'_>,
        prep_result: &PrepStepResult,
    ) -> Result<Option<DataFrame>, PollingIngestError> {
        let input_data_path = prep_result.data.path(&self.cache_dir);

        if !input_data_path.exists() || input_data_path.metadata().int_err()?.len() == 0 {
            tracing::info!(path = ?input_data_path, "Early return due to an empty file");
            return Ok(None);
        }

        let temp_path = args.operation_dir.join("reader.tmp");
        let reader = self
            .data_format_registry
            .get_reader(
                args.ctx.clone(),
                args.polling_source.read.clone(),
                temp_path,
            )
            .await?;

        let df = reader.read(&input_data_path).await?;

        Ok(Some(df))
    }

    // TODO: Introduce intermediate structs to avoid full unpacking
    fn merge_results(
        combined_result: Option<PollingIngestResult>,
        new_result: PollingIngestResult,
    ) -> PollingIngestResult {
        match (combined_result, new_result) {
            (None | Some(PollingIngestResult::UpToDate { .. }), n) => n,
            (
                Some(PollingIngestResult::Updated {
                    old_head, new_head, ..
                }),
                PollingIngestResult::UpToDate { uncacheable, .. },
            ) => PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more: false,
                uncacheable,
            },
            (
                Some(PollingIngestResult::Updated {
                    old_head: prev_old_head,
                    ..
                }),
                PollingIngestResult::Updated {
                    new_head,
                    has_more,
                    uncacheable,
                    ..
                },
            ) => PollingIngestResult::Updated {
                old_head: prev_old_head,
                new_head,
                has_more,
                uncacheable,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PollingIngestService for PollingIngestServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_active_polling_source(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetPollingSource>)>, GetDatasetError> {
        let dataset = self.dataset_repo.find_dataset_by_ref(dataset_ref).await?;

        // TODO: Support source evolution
        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetPollingSourceVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: PollingIngestOptions,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        self.do_ingest(dataset_ref, options, |_| maybe_listener)
            .await
    }

    #[tracing::instrument(level = "info", skip_all, fields(?dataset_refs))]
    async fn ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: PollingIngestOptions,
        maybe_multi_listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Vec<PollingIngestResponse> {
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullPollingIngestMultiListener));

        let futures: Vec<_> = dataset_refs
            .iter()
            .map(|dataset_ref| {
                self.do_ingest(dataset_ref, options.clone(), |hdl| {
                    multi_listener.begin_ingest(hdl)
                })
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        dataset_refs
            .into_iter()
            .zip(results)
            .map(|(dataset_ref, result)| PollingIngestResponse {
                dataset_ref,
                result,
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) enum FetchStepResult {
    UpToDate,
    Updated(FetchSavepoint),
}

pub(crate) struct PrepStepResult {
    pub data: SavepointData,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IngestLoopArgs {
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    options: PollingIngestOptions,
    listener: Arc<dyn PollingIngestListener>,
}

struct IngestIterationArgs<'a> {
    dataset_handle: DatasetHandle,
    iteration: usize,
    operation_id: String,
    operation_dir: PathBuf,
    system_time: DateTime<Utc>,
    options: PollingIngestOptions,
    polling_source: SetPollingSource,
    listener: Arc<dyn PollingIngestListener>,
    ctx: SessionContext,
    data_writer: &'a mut DataWriterDataFusion,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
