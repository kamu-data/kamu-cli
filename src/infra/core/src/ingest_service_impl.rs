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
use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, SessionContext};
use dill::*;
use kamu_core::*;
use kamu_ingest_datafusion::{
    DataWriter,
    DataWriterDataFusion,
    Reader,
    WriteDataError,
    WriteDataOpts,
};
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use super::ingest::*;

///////////////////////////////////////////////////////////////////////////////

pub struct IngestServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    container_runtime: Arc<ContainerRuntime>,
    run_info_dir: PathBuf,
    cache_dir: PathBuf,
    time_source: Arc<dyn SystemTimeSource>,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        container_runtime: Arc<ContainerRuntime>,
        run_info_dir: PathBuf,
        cache_dir: PathBuf,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            engine_provisioner,
            object_store_registry,
            container_runtime,
            run_info_dir,
            cache_dir,
            time_source,
        }
    }

    async fn do_ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: IngestOptions,
        fetch_override: Option<FetchStep>,
        get_listener: impl FnOnce(&DatasetHandle) -> Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(&dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Write)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let listener =
            get_listener(&dataset_handle).unwrap_or_else(|| Arc::new(NullIngestListener));

        // TODO: Remove this scan after we eliminate Spark ingest
        let Some((_, polling_source_block)) = dataset
            .as_metadata_chain()
            .last_of_type::<SetPollingSource>()
            .await
            .int_err()?
        else {
            tracing::warn!(
                %dataset_handle,
                "Dataset does not define a polling source - considering up-to-date",
            );

            let result = IngestResult::UpToDate {
                no_polling_source: true,
                uncacheable: false,
            };

            listener.begin();
            listener.success(&result);
            return Ok(result);
        };

        // TODO: Temporarily we continue to default to Spark ingest, but use new engine
        // if preprocessing query specifies datafusion
        let engine = polling_source_block
            .event
            .preprocess
            .as_ref()
            .map(|Transform::Sql(t)| t.engine.as_str());

        if engine == Some("datafusion") {
            self.ingest_loop(IngestLoopArgs {
                dataset_handle,
                dataset,
                options,
                polling_source: polling_source_block.event,
                fetch_override,
                listener,
            })
            .await
        } else {
            self.legacy_spark_ingest(dataset_handle, dataset, options, fetch_override, listener)
                .await
        }
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            dataset_handle = %args.dataset_handle,
        )
    )]
    async fn ingest_loop(&self, args: IngestLoopArgs) -> Result<IngestResult, IngestError> {
        let ctx = self.new_session_context();

        let mut data_writer = DataWriterDataFusion::builder(args.dataset.clone(), ctx.clone())
            .with_metadata_state_scanned()
            .await?
            .build()
            .await?;

        let mut iteration = 0;
        let mut combined_result = None;
        loop {
            iteration += 1;
            let operation_id = Self::next_operation_id();

            let operation_dir = self.run_info_dir.join(format!("ingest-{}", operation_id));
            std::fs::create_dir_all(&operation_dir).int_err()?;

            // TODO: Avoid excessive cloning
            let iteration_args = IngestIterationArgs {
                iteration,
                operation_id,
                operation_dir,
                system_time: self.time_source.now(),
                options: args.options.clone(),
                polling_source: args.polling_source.clone(),
                fetch_override: args.fetch_override.clone(),
                listener: args.listener.clone(),
                ctx: &ctx,
                data_writer: &mut data_writer,
            };

            match self.ingest_iteration(iteration_args).await {
                Ok(res) => {
                    combined_result = Some(Self::merge_results(combined_result, res));

                    let has_more = match combined_result {
                        Some(IngestResult::UpToDate { .. }) => false,
                        Some(IngestResult::Updated { has_more, .. }) => has_more,
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
    ) -> Result<IngestResult, IngestError> {
        tracing::info!(?args.options, ?args.fetch_override, "Ingest iteration details");

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
    ) -> Result<IngestResult, IngestError> {
        args.listener
            .on_stage_progress(IngestStage::CheckCache, 0, TotalSteps::Exact(1));

        let uncacheable = args.data_writer.last_offset().is_some()
            && args.data_writer.last_source_state().is_none()
            && args.fetch_override.is_none();

        if uncacheable && !args.options.fetch_uncacheable {
            tracing::info!("Skipping fetch of uncacheable source");
            return Ok(IngestResult::UpToDate {
                no_polling_source: false,
                uncacheable,
            });
        }

        let savepoint = match self.fetch(&args).await? {
            FetchStepResult::Updated(savepoint) => savepoint,
            FetchStepResult::UpToDate => {
                return Ok(IngestResult::UpToDate {
                    no_polling_source: false,
                    uncacheable,
                })
            }
        };

        args.listener
            .on_stage_progress(IngestStage::Prepare, 0, TotalSteps::Exact(1));

        let prepare_result = self.prepare(&args, &savepoint).await?;

        args.listener
            .on_stage_progress(IngestStage::Read, 0, TotalSteps::Exact(1));

        let df = self.read(&args, &prepare_result).await?;

        args.listener
            .on_stage_progress(IngestStage::Preprocess, 0, TotalSteps::Exact(1));

        let df = if let Some(df) = df {
            let df = self.preprocess(&args, df).await?;
            self.validate_new_data(&df, args.data_writer.vocab())?;
            Some(df)
        } else {
            None
        };

        args.listener
            .on_stage_progress(IngestStage::Merge, 0, TotalSteps::Exact(1));
        args.listener
            .on_stage_progress(IngestStage::Commit, 0, TotalSteps::Exact(1));

        let source_state = savepoint.source_state.map(|ss| ss.to_source_state());

        let out_dir = args.operation_dir.join("out");
        let data_staging_path = out_dir.join("data");
        std::fs::create_dir(&out_dir).int_err()?;

        let res = args
            .data_writer
            .write(
                df,
                WriteDataOpts {
                    system_time: args.system_time.clone(),
                    source_event_time: savepoint
                        .source_event_time
                        .clone()
                        .unwrap_or(args.system_time.clone()),
                    source_state,
                    data_staging_path,
                },
            )
            .await;

        // Clean up intermediate files
        // Note that we are leaving the fetch data and savepoint intact
        // in case user wants to iterate on the dataset.
        if prepare_result.data_cache_key != savepoint.data_cache_key {
            std::fs::remove_file(self.cache_dir.join(prepare_result.data_cache_key)).int_err()?;
        }

        match res {
            Ok(res) => Ok(IngestResult::Updated {
                old_head: res.old_head,
                new_head: res.new_head,
                num_blocks: 1,
                has_more: savepoint.has_more,
                uncacheable,
            }),
            Err(WriteDataError::EmptyCommit(_)) => Ok(IngestResult::UpToDate {
                no_polling_source: false,
                uncacheable,
            }),
            Err(WriteDataError::MergeError(e)) => Err(e.into()),
            Err(WriteDataError::CommitError(e)) => Err(e.into()),
            Err(WriteDataError::Internal(e)) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn fetch(&self, args: &IngestIterationArgs<'_>) -> Result<FetchStepResult, IngestError> {
        // Ignore source state for overridden fetch step
        let (fetch_step, prev_source_state) = if let Some(fetch_override) = &args.fetch_override {
            (fetch_override, None)
        } else {
            (
                &args.polling_source.fetch,
                args.data_writer.last_source_state(),
            )
        };

        let prev_source_state =
            prev_source_state.and_then(|ss| PollingSourceState::try_from_source_state(&ss));

        let savepoint_path = self.get_savepoint_path(fetch_step, prev_source_state.as_ref())?;
        let savepoint = self.read_fetch_savepoint(&savepoint_path)?;

        if let Some(savepoint) = savepoint {
            tracing::info!(?savepoint_path, "Resuming from savepoint");

            args.listener.on_cache_hit(&savepoint.created_at);

            Ok(FetchStepResult::Updated(savepoint))
        } else {
            // Just in case user deleted it manually
            if !self.cache_dir.exists() {
                std::fs::create_dir(&self.cache_dir).int_err()?;
            }

            let data_cache_key = self.get_random_cache_key("fetch-");
            let target_path = self.cache_dir.join(&data_cache_key);

            let fetch_service =
                FetchService::new(self.container_runtime.clone(), &self.run_info_dir);

            let fetch_result = fetch_service
                .fetch(
                    &args.operation_id,
                    fetch_step,
                    prev_source_state.as_ref(),
                    &target_path,
                    &args.system_time,
                    Some(Arc::new(FetchProgressListenerBridge::new(
                        args.listener.clone(),
                    ))),
                )
                .await?;

            match fetch_result {
                FetchResult::UpToDate => Ok(FetchStepResult::UpToDate),
                FetchResult::Updated(upd) => {
                    let savepoint = FetchSavepoint {
                        created_at: args.system_time.clone(),
                        // If fetch source was overridden we don't want to put its
                        // source state into the metadata.
                        source_state: if args.fetch_override.is_none() {
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn prepare(
        &self,
        args: &IngestIterationArgs<'_>,
        fetch_result: &FetchSavepoint,
    ) -> Result<PrepStepResult, IngestError> {
        let null_steps: Vec<_> = Vec::new();
        let prep_steps = args.polling_source.prepare.as_ref().unwrap_or(&null_steps);

        if prep_steps.is_empty() {
            Ok(PrepStepResult {
                // Specify input as output
                data_cache_key: fetch_result.data_cache_key.clone(),
            })
        } else {
            let src_path = self.cache_dir.join(&fetch_result.data_cache_key);
            let data_cache_key = self.get_random_cache_key("prepare-");
            let target_path = self.cache_dir.join(&data_cache_key);

            let prep_service = PrepService::new();
            prep_service.prepare(prep_steps, &src_path, &target_path)?;

            Ok(PrepStepResult { data_cache_key })
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(
        &self,
        args: &IngestIterationArgs<'_>,
        prep_result: &PrepStepResult,
    ) -> Result<Option<DataFrame>, IngestError> {
        let input_data_path = self.cache_dir.join(&prep_result.data_cache_key);

        if input_data_path.metadata().int_err()?.len() == 0 {
            return Ok(None);
        }

        let reader = Self::get_reader_for(&args.polling_source.read, &args.operation_dir);

        let df = reader
            .read(&args.ctx, &input_data_path, &args.polling_source.read)
            .await?;

        Ok(Some(df))
    }

    // TODO: Replace with DI
    fn get_reader_for(conf: &ReadStep, operation_dir: &Path) -> Arc<dyn Reader> {
        use kamu_ingest_datafusion::readers::*;

        let temp_path = operation_dir.join("reader.tmp");
        match conf {
            ReadStep::Csv(_) => Arc::new(ReaderCsv {}),
            ReadStep::Json(_) => Arc::new(ReaderJson::new(temp_path)),
            ReadStep::NdJson(_) => Arc::new(ReaderNdJson {}),
            ReadStep::JsonLines(_) => Arc::new(ReaderNdJson {}),
            ReadStep::GeoJson(_) => Arc::new(ReaderGeoJson::new(temp_path)),
            ReadStep::NdGeoJson(_) => Arc::new(ReaderNdGeoJson::new(temp_path)),
            ReadStep::EsriShapefile(_) => Arc::new(ReaderEsriShapefile::new(temp_path)),
            ReadStep::Parquet(_) => Arc::new(ReaderParquet {}),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn preprocess(
        &self,
        args: &IngestIterationArgs<'_>,
        df: DataFrame,
    ) -> Result<DataFrame, IngestError> {
        let Some(preprocess) = args.polling_source.preprocess.clone() else {
            return Ok(df);
        };

        let Transform::Sql(preprocess) = preprocess;

        // TODO: Support other engines
        assert_eq!(preprocess.engine.to_lowercase(), "datafusion");

        let preprocess = preprocess.normalize_queries(Some("output".to_string()));

        // Setup input
        args.ctx.register_table("input", df.into_view()).int_err()?;

        // Setup queries
        for query_step in preprocess.queries.unwrap_or_default() {
            self.register_view_for_step(args.ctx, &query_step).await?;
        }

        // Get result's execution plan
        let df = args.ctx.table("output").await.int_err()?;

        tracing::debug!(
            schema = ?df.schema(),
            logical_plan = ?df.logical_plan(),
            "Performing preprocess step",
        );

        Ok(df)
    }

    async fn register_view_for_step(
        &self,
        ctx: &SessionContext,
        step: &SqlQueryStep,
    ) -> Result<(), EngineError> {
        use datafusion::logical_expr::*;
        use datafusion::sql::TableReference;

        let name = step.alias.as_ref().unwrap();

        tracing::debug!(
            %name,
            query = %step.query,
            "Creating view for a query",
        );

        let logical_plan = match ctx.state().create_logical_plan(&step.query).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::error!(
                    error = &error as &dyn std::error::Error,
                    query = %step.query,
                    "Error when setting up query"
                );
                return Err(InvalidQueryError::new(error.to_string(), Vec::new()).into());
            }
        };

        let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: TableReference::bare(step.alias.as_ref().unwrap()).to_owned_reference(),
            input: Arc::new(logical_plan),
            or_replace: false,
            definition: Some(step.query.clone()),
        }));

        ctx.execute_logical_plan(create_view).await.int_err()?;
        Ok(())
    }

    fn validate_new_data(
        &self,
        df: &DataFrame,
        vocab: &DatasetVocabularyResolved<'_>,
    ) -> Result<(), EngineError> {
        use datafusion::arrow::datatypes::DataType;

        let system_columns = [&vocab.offset_column, &vocab.system_time_column];
        for system_column in system_columns {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Err(InvalidQueryError::new(
                    format!(
                        "Transformed data contains a column that conflicts with the system column \
                         name, you should either rename the data column or configure the dataset \
                         vocabulary to use a different name: {}",
                        system_column
                    ),
                    Vec::new(),
                )
                .into());
            }
        }

        // Event time: If present must be a TIMESTAMP or DATE
        let event_time_col = match df
            .schema()
            .field_with_unqualified_name(&vocab.event_time_column)
        {
            Ok(f) => Some(f),
            Err(DataFusionError::SchemaError(SchemaError::FieldNotFound { .. })) => None,
            Err(err) => return Err(err.int_err().into()),
        };

        if let Some(event_time_col) = event_time_col {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 => {}
                DataType::Timestamp(_, _) => {}
                typ => {
                    return Err(InvalidQueryError::new(
                        format!(
                            "Event time column '{}' should be either Date or Timestamp, but \
                             found: {}",
                            vocab.event_time_column, typ
                        ),
                        Vec::new(),
                    )
                    .into());
                }
            }
        };

        Ok(())
    }

    fn next_operation_id() -> String {
        use rand::distributions::Alphanumeric;
        use rand::Rng;

        let mut name = String::with_capacity(16);
        name.extend(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from),
        );

        name
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

    fn new_session_context(&self) -> SessionContext {
        use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
        use datafusion::prelude::*;

        let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");

        let runtime_config = RuntimeConfig {
            object_store_registry: self.object_store_registry.clone().as_datafusion_registry(),
            ..RuntimeConfig::default()
        };

        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

        SessionContext::with_config_rt(config, runtime)
    }

    // TODO: Introduce intermediate structs to avoid full unpacking
    fn merge_results(
        combined_result: Option<IngestResult>,
        new_result: IngestResult,
    ) -> IngestResult {
        match (combined_result, new_result) {
            (None, n) => n,
            (Some(IngestResult::UpToDate { .. }), n) => n,
            (
                Some(IngestResult::Updated {
                    old_head,
                    new_head,
                    num_blocks,
                    ..
                }),
                IngestResult::UpToDate {
                    no_polling_source: _,
                    uncacheable,
                },
            ) => IngestResult::Updated {
                old_head,
                new_head,
                num_blocks,
                has_more: false,
                uncacheable,
            },
            (
                Some(IngestResult::Updated {
                    old_head: prev_old_head,
                    num_blocks: prev_num_blocks,
                    ..
                }),
                IngestResult::Updated {
                    new_head,
                    num_blocks,
                    has_more,
                    uncacheable,
                    ..
                },
            ) => IngestResult::Updated {
                old_head: prev_old_head,
                new_head,
                num_blocks: num_blocks + prev_num_blocks,
                has_more,
                uncacheable,
            },
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Legacy Spark-based ingest path
///////////////////////////////////////////////////////////////////////////////

impl IngestServiceImpl {
    async fn legacy_spark_ingest(
        &self,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
        options: IngestOptions,
        fetch_override: Option<FetchStep>,
        listener: Arc<dyn IngestListener>,
    ) -> Result<IngestResult, IngestError> {
        let request = self
            .legacy_prepare_ingest_request(dataset_handle, dataset.clone())
            .await?;

        // TODO: create via DI to avoid passing through all dependencies
        let ingest_task = IngestTask::new(
            dataset,
            request,
            options.clone(),
            fetch_override,
            listener,
            self.engine_provisioner.clone(),
            self.container_runtime.clone(),
            &self.run_info_dir,
            &self.cache_dir,
        )
        .await?;

        Self::legacy_poll_until_exhausted(ingest_task, options).await
    }

    async fn legacy_poll_until_exhausted(
        mut task: IngestTask,
        options: IngestOptions,
    ) -> Result<IngestResult, IngestError> {
        let mut combined_result = None;

        loop {
            match task.ingest(Self::next_operation_id()).await {
                Ok(res) => {
                    combined_result = Some(Self::merge_results(combined_result, res));

                    let has_more = match combined_result {
                        Some(IngestResult::UpToDate { .. }) => false,
                        Some(IngestResult::Updated { has_more, .. }) => has_more,
                        None => unreachable!(),
                    };

                    if !has_more || !options.exhaust_sources {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(combined_result.unwrap())
    }

    async fn legacy_prepare_ingest_request(
        &self,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
    ) -> Result<IngestRequest, InternalError> {
        // TODO: PERF: Full metadata scan below - this is expensive and should be cached
        let mut polling_source = None;
        let mut prev_source_state = None;
        let mut prev_data_slices = Vec::new();
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
                        if let Some(output_data) = &add_data.output_data {
                            prev_data_slices.push(output_data.physical_hash.clone());

                            if next_offset.is_none() {
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
                            // TODO: Should we check that this is polling source?
                            prev_source_state = Some(add_data.source_state);
                        }
                    }
                    MetadataEvent::SetWatermark(set_wm) => {
                        if prev_watermark.is_none() {
                            prev_watermark = Some(Some(set_wm.output_watermark));
                        }
                    }
                    MetadataEvent::SetPollingSource(src) => {
                        if polling_source.is_none() {
                            polling_source = Some(src);
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
            }
        }

        Ok(IngestRequest {
            operation_id: "".to_string(), // TODO: Will be filled out by IngestTask
            dataset_handle,
            polling_source: polling_source.unwrap(),
            system_time: self.time_source.now(),
            event_time: None, // TODO: Will be filled out by IngestTask
            input_data_path: PathBuf::new(), // TODO: Will be filled out by IngestTask
            prev_data_slices,
            next_offset: next_offset.unwrap_or_default(),
            vocab: vocab.unwrap_or_default(),
            prev_checkpoint: prev_checkpoint.unwrap_or_default(),
            prev_watermark: prev_watermark.unwrap_or_default(),
            prev_source_state: prev_source_state.clone().unwrap_or_default(),
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl IngestService for IngestServiceImpl {
    async fn ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        tracing::info!(%dataset_ref, "Ingesting single dataset");
        self.do_ingest(dataset_ref, options, None, |_| maybe_listener)
            .await
    }

    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRef,
        fetch: FetchStep,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        tracing::info!(%dataset_ref, ?fetch, "Ingesting single dataset from overriden source");
        self.do_ingest(dataset_ref, options, Some(fetch), |_| maybe_listener)
            .await
    }

    async fn ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        self.ingest_multi_ext(
            dataset_refs
                .into_iter()
                .map(|r| IngestParams {
                    dataset_ref: r,
                    fetch_override: None,
                })
                .collect(),
            options,
            maybe_multi_listener,
        )
        .await
    }

    async fn ingest_multi_ext(
        &self,
        requests: Vec<IngestParams>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullIngestMultiListener));

        tracing::info!(?requests, "Ingesting multiple datasets");

        let futures: Vec<_> = requests
            .iter()
            .map(|req| {
                self.do_ingest(
                    &req.dataset_ref,
                    options.clone(),
                    req.fetch_override.clone(),
                    |hdl| multi_listener.begin_ingest(hdl),
                )
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        requests
            .into_iter()
            .map(|r| r.dataset_ref)
            .zip(results)
            .collect()
    }
}

struct IngestLoopArgs {
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    options: IngestOptions,
    polling_source: SetPollingSource,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn IngestListener>,
}

struct IngestIterationArgs<'a> {
    iteration: usize,
    operation_id: String,
    operation_dir: PathBuf,
    system_time: DateTime<Utc>,
    options: IngestOptions,
    polling_source: SetPollingSource,
    fetch_override: Option<FetchStep>,
    listener: Arc<dyn IngestListener>,
    ctx: &'a SessionContext,
    data_writer: &'a mut DataWriterDataFusion,
}
