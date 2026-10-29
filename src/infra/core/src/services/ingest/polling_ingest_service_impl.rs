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
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::ingest::*;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use kamu_ingest_datafusion::DataWriterDataFusion;
use odf::dataset::MetadataChainExt;
use odf::metadata::serde::yaml::legacy::Manifest;
use odf::utils::data::DataFrameExt;
use random_strings::get_random_name;
use time_source::SystemTimeSource;

use super::*;
use crate::EngineConfigDatafusionEmbeddedIngest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn PollingIngestService)]
pub struct PollingIngestServiceImpl {
    fetch_service: Arc<FetchService>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    data_format_registry: Arc<dyn DataFormatRegistry>,
    engine_config: Arc<EngineConfigDatafusionEmbeddedIngest>,
    run_info_dir: Arc<RunInfoDir>,
    cache_dir: Arc<CacheDir>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PollingIngestServiceImpl {
    async fn ingest_inner(
        &self,
        target: ResolvedDataset,
        plan: PollingIngestPlan,
        options: PollingIngestOptions,
        env_vars: &IngestEnvVars,
        operation_id: &str,
        listener: Arc<dyn PollingIngestListener>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        let polling_source = plan.polling_source().clone();

        let ctx = ingest_common::new_session_context(
            &self.engine_config,
            self.object_store_registry.clone(),
        );
        let mut data_writer = DataWriterDataFusion::from_metadata_state(
            ctx.clone(),
            target.clone(),
            *plan.into_metadata_state(),
        );

        let operation_dir = self.run_info_dir.join(format!("ingest-{operation_id}"));
        std::fs::create_dir_all(&operation_dir).int_err()?;

        let new_ctx = ingest_common::new_session_context(
            &self.engine_config,
            self.object_store_registry.clone(),
        );
        data_writer.set_session_context(new_ctx.clone());

        let iteration_args = IngestIterationArgs {
            target,
            operation_id: operation_id.to_string(),
            operation_dir: operation_dir.clone(),
            system_time: self.time_source.now(),
            options,
            env_vars,
            polling_source,
            listener,
            ctx: new_ctx,
            data_writer: &mut data_writer,
        };

        let response = self.ingest_iteration(iteration_args).await?;

        // A failed attempt keeps its directory: errors point at the logs in it
        if let Err(err) = std::fs::remove_dir_all(&operation_dir) {
            tracing::warn!(error = %err, ?operation_dir, "Failed to remove the ingest directory");
        }

        Ok(response)
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
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

        args.listener.begin();

        match self.ingest_iteration_inner(args).await {
            Ok(res) => {
                match &res {
                    PollingIngestResult::UpToDate { uncacheable } => {
                        tracing::info!(uncacheable, "Ingest iteration successful, up to date");
                    }
                    PollingIngestResult::Updated {
                        old_head,
                        new_head,
                        has_more,
                        increment,
                        ..
                    } => tracing::info!(
                        %old_head,
                        %new_head,
                        has_more,
                        ?increment,
                        "Ingest iteration successful",
                    ),
                }
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, error_msg = %err, "Ingest iteration failed");
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
            && !matches!(args.polling_source.fetch, odf::metadata::FetchStep::Mqtt(_));

        if uncacheable && !args.options.fetch_uncacheable {
            tracing::info!("Skipping fetch of uncacheable source");
            return Ok(PollingIngestResult::UpToDate { uncacheable });
        }

        let savepoint = match self.fetch(&args).await? {
            FetchStepResult::Updated(savepoint) => savepoint,
            FetchStepResult::UpToDate => {
                return Ok(PollingIngestResult::UpToDate { uncacheable });
            }
        };

        args.listener
            .on_stage_progress(PollingIngestStage::Prepare, 0, TotalSteps::Exact(1));

        let prepare_result = self
            .prepare(&args, &savepoint, self.run_info_dir.clone())
            .await?;

        let stage_result = self
            .read_and_stage(&args, &savepoint, &prepare_result)
            .await;

        // Clean up intermediate files
        // Note that we are leaving the fetch data and savepoint intact
        // in case user wants to iterate on the dataset.
        if prepare_result.data != savepoint.data
            && let Err(err) = prepare_result.data.remove_owned(&self.cache_dir)
        {
            tracing::warn!(error = %err, "Failed to remove the prepared data");
        }

        match stage_result? {
            Ok(staged) => {
                args.listener.on_stage_progress(
                    PollingIngestStage::Commit,
                    0,
                    TotalSteps::Exact(1),
                );

                let writer_res = args.data_writer.commit(staged).await?;
                let new_source_state = writer_res
                    .add_data_block
                    .as_ref()
                    .and_then(|block| block.event.new_source_state.clone());

                let increment = args
                    .target
                    .as_metadata_chain()
                    .get_increment_for_interval(Some(&writer_res.old_head), &writer_res.new_head)
                    .await
                    .int_err()?;

                Ok(PollingIngestResult::Updated {
                    old_head: writer_res.old_head,
                    new_head: writer_res.new_head,
                    has_more: savepoint.has_more,
                    uncacheable,
                    increment,
                    new_source_state,
                    metadata_state: Box::new(args.data_writer.as_metadata_state()),
                })
            }
            Err(StageDataError::BadInputSchema(e)) => Err(e.into()),
            Err(StageDataError::IncompatibleSchema(e)) => Err(e.into()),
            Err(StageDataError::MergeError(e)) => Err(e.into()),
            Err(StageDataError::ExecutionError(e)) => Err(e.into()),
            Err(StageDataError::DataValidation(e)) => Err(e.into()),
            Err(StageDataError::EmptyCommit(_)) => {
                Ok(PollingIngestResult::UpToDate { uncacheable })
            }
            Err(StageDataError::Internal(e)) => Err(e.into()),
        }
    }

    /// Reads the prepared data, preprocesses it and stages the writer output.
    /// The outer error is a failure before staging.
    async fn read_and_stage(
        &self,
        args: &IngestIterationArgs<'_>,
        savepoint: &FetchSavepoint,
        prepare_result: &PrepStepResult,
    ) -> Result<Result<StageDataResult, StageDataError>, PollingIngestError> {
        args.listener
            .on_stage_progress(PollingIngestStage::Read, 0, TotalSteps::Exact(1));

        let df = if let Some(df) = self.read(args, prepare_result).await? {
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
                Some(
                    ingest_common::preprocess_default(
                        df,
                        &args.polling_source.read,
                        &args.polling_source.merge,
                        args.data_writer.vocab(),
                        &args.options.schema_inference,
                    )
                    .int_err()?,
                )
            }
        } else {
            tracing::info!("Read did not produce a data frame");
            None
        };

        let new_source_state = savepoint
            .source_state
            .as_ref()
            .map(PollingSourceState::to_source_state);

        let out_dir = args.operation_dir.join("out");
        let data_staging_path = out_dir.join("data.parquet");
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

        tracing::info!(?stage_result, "Stage result");

        Ok(stage_result)
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
            } else if let odf::metadata::FetchStep::Mqtt(_) = fetch_step {
            } else if let SavepointData::Ref { .. } = savepoint.data {
                // The referenced file may have changed since the savepoint was written
                tracing::info!(?savepoint_path, "Ignoring savepoint of a zero-copy fetch");
            } else if !savepoint.data.path(&self.cache_dir).exists() {
                // Resuming would commit the savepoint's source state without its data
                tracing::warn!(?savepoint_path, "Ignoring savepoint whose data is gone");
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

        // Note: Boxing the fetch service to reduce stack usage
        let fetch_result = Box::pin(self.fetch_service.fetch(
            args.target.get_handle(),
            &args.operation_id,
            fetch_step,
            prev_source_state.as_ref(),
            &target_path,
            &args.system_time,
            args.env_vars.as_map(),
            Some(Arc::new(FetchProgressListenerBridge::new(
                args.listener.clone(),
            ))),
        ))
        .await;

        match fetch_result {
            Err(e) => {
                Self::discard_fetch_target(&target_path);
                Err(e)
            }
            Ok(FetchResult::UpToDate) => {
                Self::discard_fetch_target(&target_path);
                Ok(FetchStepResult::UpToDate)
            }
            Ok(FetchResult::Updated(upd)) => {
                let (data, keep_savepoint) = if let Some(path) = upd.zero_copy_path {
                    Self::discard_fetch_target(&target_path);
                    (SavepointData::Ref { path }, false)
                } else {
                    let data = SavepointData::Owned {
                        cache_key: data_cache_key,
                    };
                    (data, true)
                };

                let savepoint = FetchSavepoint {
                    created_at: args.system_time,
                    source_state: upd.source_state,
                    source_event_time: upd.source_event_time,
                    data,
                    has_more: upd.has_more,
                };
                // A zero-copy fetch is free to repeat, and resuming one would
                // pair the source state it recorded with whatever the file
                // holds by then
                if keep_savepoint {
                    self.write_fetch_savepoint(&savepoint_path, &savepoint)?;
                }
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
        fetch_step: &odf::metadata::FetchStep,
        source_state: Option<&PollingSourceState>,
    ) -> PathBuf {
        use odf::metadata::serde::flatbuffers::{
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

        let hash = odf::Multihash::from_digest_sha3_256(fb.finished_data());

        self.cache_dir.join(format!("fetch-savepoint-{hash}"))
    }

    /// Removes a fetch target that no savepoint refers to
    fn discard_fetch_target(target_path: &Path) {
        if target_path.exists()
            && let Err(err) = std::fs::remove_file(target_path)
        {
            tracing::warn!(error = %err, ?target_path, "Failed to remove the fetch target");
        }
    }

    /// A savepoint that does not parse is ignored, so the source is fetched
    /// again instead of failing every run until the cache is cleared
    fn read_fetch_savepoint(
        &self,
        savepoint_path: &Path,
    ) -> Result<Option<FetchSavepoint>, InternalError> {
        if !savepoint_path.is_file() {
            return Ok(None);
        }

        let file = std::fs::File::open(savepoint_path).int_err()?;
        match serde_yaml::from_reader::<_, Manifest<FetchSavepoint>>(file) {
            Ok(manifest) => Ok(Some(manifest.content)),
            Err(err) => {
                tracing::warn!(error = %err, ?savepoint_path, "Ignoring unreadable savepoint");
                Ok(None)
            }
        }
    }

    /// Writes to a temporary file first, so a failed write never leaves a
    /// truncated savepoint behind
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

        let tmp_path = self
            .cache_dir
            .join(get_random_name(Some("fetch-savepoint-tmp-"), 10));
        let file = std::fs::File::create(&tmp_path).int_err()?;
        serde_yaml::to_writer(&file, &manifest).int_err()?;
        file.sync_all().int_err()?;
        std::fs::rename(&tmp_path, savepoint_path).int_err()?;

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
    ) -> Result<Option<DataFrameExt>, PollingIngestError> {
        let input_data_path = prep_result.data.path(&self.cache_dir);

        let temp_path = args.operation_dir.join("reader.tmp");
        let reader = self
            .data_format_registry
            .get_reader(
                args.ctx.clone(),
                args.polling_source.read.clone(),
                odf::schema::ToArrowSettings::default(),
                temp_path,
            )
            .await?;

        let input_is_missing_or_empty =
            !input_data_path.exists() || input_data_path.metadata().int_err()?.len() == 0;

        if input_is_missing_or_empty {
            if let Some(read_schema) = reader.schema_arrow() {
                tracing::info!(
                    path = ?input_data_path,
                    "Returning an empty data frame as input file is empty",
                );

                let df = args
                    .ctx
                    .read_batch(RecordBatch::new_empty(read_schema.clone()))
                    .int_err()?;

                return Ok(Some(df.into()));
            }

            tracing::info!(
                path = ?input_data_path,
                "Skipping ingest due to an empty file and empty read schema",
            );
            return Ok(None);
        }

        let df = reader.read(&input_data_path).await?;

        Ok(Some(df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PollingIngestService for PollingIngestServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn ingest(
        &self,
        target: ResolvedDataset,
        plan: PollingIngestPlan,
        options: PollingIngestOptions,
        env_vars: &IngestEnvVars,
        operation_id: &str,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullPollingIngestListener));

        self.ingest_inner(target, plan, options, env_vars, operation_id, listener)
            .await
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

struct IngestIterationArgs<'a> {
    target: ResolvedDataset,
    operation_id: String,
    operation_dir: PathBuf,
    system_time: DateTime<Utc>,
    options: PollingIngestOptions,
    env_vars: &'a IngestEnvVars,
    polling_source: odf::metadata::SetPollingSource,
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
