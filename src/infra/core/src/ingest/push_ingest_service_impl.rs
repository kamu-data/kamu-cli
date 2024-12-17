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

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::ingest::*;
use kamu_core::*;
use kamu_ingest_datafusion::*;
use opendatafabric::*;
use random_names::get_random_name;
use time_source::SystemTimeSource;
use tokio::io::AsyncRead;

use super::ingest_common;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PushIngestServiceImpl {
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    data_format_registry: Arc<dyn DataFormatRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    run_info_dir: Arc<RunInfoDir>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn PushIngestService)]
impl PushIngestServiceImpl {
    pub fn new(
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        data_format_registry: Arc<dyn DataFormatRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            object_store_registry,
            data_format_registry,
            time_source,
            engine_provisioner,
            run_info_dir,
        }
    }

    async fn prepare_metadata_state(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
    ) -> Result<DataWriterMetadataState, PushIngestPlanningError> {
        let metadata_state = DataWriterMetadataState::build(target, &BlockRef::Head, source_name)
            .await
            .map_err(|e| match e {
                ScanMetadataError::SourceNotFound(err) => {
                    PushIngestPlanningError::SourceNotFound(err.into())
                }
                ScanMetadataError::Internal(err) => PushIngestPlanningError::Internal(err),
            })?;
        Ok(metadata_state)
    }

    async fn auto_create_push_source(
        &self,
        target: ResolvedDataset,
        source_name: &str,
        opts: &PushIngestOpts,
    ) -> Result<AddPushSource, PushIngestPlanningError> {
        let read = match &opts.media_type {
            Some(media_type) => {
                match self
                    .data_format_registry
                    .get_best_effort_config(None, media_type)
                {
                    Ok(read_step) => Ok(read_step),
                    Err(e) => Err(PushIngestPlanningError::UnsupportedMediaType(e)),
                }
            }
            None => Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(Some(source_name)),
            )),
        }?;

        let add_push_source_event = AddPushSource {
            source_name: String::from("auto"),
            read,
            preprocess: None,
            merge: opendatafabric::MergeStrategy::Append(opendatafabric::MergeStrategyAppend {}),
        };

        let commit_result = target
            .commit_event(
                MetadataEvent::AddPushSource(add_push_source_event.clone()),
                CommitOpts {
                    system_time: opts.source_event_time,
                    ..CommitOpts::default()
                },
            )
            .await;
        match commit_result {
            Ok(_) => Ok(add_push_source_event),
            Err(e) => Err(PushIngestPlanningError::CommitError(e)),
        }
    }

    async fn do_ingest(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        source: DataSource,
        listener: Arc<dyn PushIngestListener>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let ctx: SessionContext =
            ingest_common::new_session_context(self.object_store_registry.clone());

        let data_writer = DataWriterDataFusion::builder(target.clone(), ctx.clone())
            .with_metadata_state(*plan.metadata_state)
            .build();

        listener.begin();

        match self
            .do_ingest_inner(plan.args, source, data_writer, ctx, listener.clone())
            .await
        {
            Ok(res) => {
                tracing::info!(result = ?res, "Ingest iteration successful");
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, error_msg = %err, "Ingest iteration failed");
                listener.error(&err);
                Err(err)
            }
        }
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            operation_id = %args.operation_id,
        )
    )]
    async fn do_ingest_inner(
        &self,
        args: PushIngestArgs,
        source: DataSource,
        mut data_writer: DataWriterDataFusion,
        ctx: SessionContext,
        listener: Arc<dyn PushIngestListener>,
    ) -> Result<PushIngestResult, PushIngestError> {
        listener.on_stage_progress(PushIngestStage::Read, 0, TotalSteps::Exact(1));

        std::fs::create_dir_all(&args.operation_dir).int_err()?;

        let input_data_path = self.maybe_fetch(source, &args).await?;

        let df = if let Some(df) = self.read(&input_data_path, &ctx, &args).await? {
            if let Some(transform) = &args.push_source.preprocess {
                listener.on_stage_progress(PushIngestStage::Preprocess, 0, TotalSteps::Exact(1));

                ingest_common::preprocess(
                    &args.operation_id,
                    self.engine_provisioner.as_ref(),
                    &ctx,
                    transform,
                    df,
                    listener.clone().get_engine_provisioning_listener(),
                )
                .await?
            } else {
                Some(
                    ingest_common::preprocess_default(
                        df,
                        &args.push_source.read,
                        data_writer.vocab(),
                        &args.opts.schema_inference,
                    )
                    .int_err()?,
                )
            }
        } else {
            tracing::info!("Read did not produce a data frame");
            None
        };

        let out_dir = args.operation_dir.join("out");
        let data_staging_path = out_dir.join("data.parquet");
        std::fs::create_dir(&out_dir).int_err()?;

        let stage_result = data_writer
            .stage(
                df,
                WriteDataOpts {
                    system_time: args.system_time,
                    source_event_time: args.opts.source_event_time.unwrap_or(args.system_time),
                    new_watermark: None,
                    new_source_state: None, // TODO: Support storing ingest source state
                    data_staging_path,
                },
            )
            .await;

        tracing::debug!(?stage_result, "Staged the write operation");

        match stage_result {
            Ok(staged) => {
                listener.on_stage_progress(PushIngestStage::Commit, 0, TotalSteps::Exact(1));

                let res = data_writer.commit(staged).await?;

                Ok(PushIngestResult::Updated {
                    old_head: res.old_head,
                    new_head: res.new_head,
                    num_blocks: 1,
                })
            }
            Err(StageDataError::BadInputSchema(e)) => Err(e.into()),
            Err(StageDataError::IncompatibleSchema(e)) => Err(e.into()),
            Err(StageDataError::MergeError(e)) => Err(e.into()),
            Err(StageDataError::EmptyCommit(_)) => Ok(PushIngestResult::UpToDate),
            Err(StageDataError::Internal(e)) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn maybe_fetch(
        &self,
        source: DataSource,
        args: &PushIngestArgs,
    ) -> Result<PathBuf, PushIngestError> {
        // TODO: Support S3
        let temp_path = args.operation_dir.join("input-data");

        match source {
            DataSource::Url(url) => {
                match url.scheme() {
                    "file" => {
                        let p = url
                            .to_file_path()
                            .map_err(|_| format!("Invalid file URL {url}").int_err())?;

                        // TODO: In case of STDIN (/dev/fd/0) or other pipes and special device
                        // files we have to copy data into a temporary file,
                        // as DataFusion cannot read from them directly.
                        cfg_if::cfg_if! {
                            if #[cfg(unix)] {
                                use std::os::unix::fs::FileTypeExt;
                                let ft = p.metadata().int_err()?.file_type();
                                if ft.is_fifo() || ft.is_char_device() || ft.is_block_device() {
                                    tracing::info!(
                                        from_path = %p.display(),
                                        to_path = %temp_path.display(),
                                        "Detected a special file type - copying into temporary path first",
                                    );
                                    Self::copy_special_file(&p, &temp_path).await?;
                                    Ok(temp_path)
                                } else {
                                    Ok(p)
                                }
                            } else {
                                Ok(p)
                            }
                        }
                    }
                    _ => Err(format!("Unsupported source: {url}").int_err().into()),
                }
            }
            DataSource::Stream(stream) => {
                // Save stream to a file so datafusion can read it
                // TODO: We likely can avoid this and build a DataFrame directly
                tracing::info!(path = ?temp_path, "Copying stream into a temp file");
                Self::copy_stream_to_file(stream, &temp_path)
                    .await
                    .int_err()?;
                Ok(temp_path)
            }
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(
        &self,
        input_data_path: &Path,
        ctx: &SessionContext,
        args: &PushIngestArgs,
    ) -> Result<Option<DataFrame>, PushIngestError> {
        let conf = if let Some(media_type) = &args.opts.media_type {
            let conf = self
                .data_format_registry
                .get_compatible_read_config(args.push_source.read.clone(), media_type)?;

            tracing::debug!(
                ?conf,
                "Proceeding with best-effort compatibility read configuration"
            );
            conf
        } else {
            args.push_source.read.clone()
        };

        let temp_path = args.operation_dir.join("reader.tmp");
        let reader = self
            .data_format_registry
            .get_reader(ctx.clone(), conf, temp_path)
            .await?;

        if input_data_path.metadata().int_err()?.len() == 0 {
            if let Some(read_schema) = reader.input_schema().await {
                tracing::info!(
                    path = ?input_data_path,
                    "Returning an empty data frame as input file is empty",
                );

                let df = ctx
                    .read_batch(RecordBatch::new_empty(read_schema))
                    .int_err()?;

                return Ok(Some(df));
            }

            tracing::info!(
                path = ?input_data_path,
                "Skipping ingest due to an empty file and empty read schema",
            );
            return Ok(None);
        }

        let df = reader.read(input_data_path).await?;
        tracing::debug!(schema = ?df.schema(), "Reader created a dataframe");

        Ok(Some(df))
    }

    // TODO: This is needed because tokio::fs::copy refuses to work with special
    // files
    async fn copy_special_file(
        source_path: &Path,
        target_path: &Path,
    ) -> Result<(), InternalError> {
        let mut source = std::fs::File::open(source_path).int_err()?;
        let mut target = std::fs::File::create(target_path).int_err()?;

        tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
            std::io::copy(&mut source, &mut target)?;
            Ok(())
        })
        .await
        .int_err()?
        .int_err()
    }

    async fn copy_stream_to_file(
        mut data: Box<dyn AsyncRead + Send + Unpin>,
        target_path: &Path,
    ) -> Result<(), std::io::Error> {
        let mut file = tokio::fs::File::create(target_path).await?;
        tokio::io::copy(&mut data, &mut file).await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PushIngestService for PushIngestServiceImpl {
    /// Uses or auto-creates push source definition in metadata to plan
    /// ingestion
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: PushIngestOpts,
    ) -> Result<PushIngestPlan, PushIngestPlanningError> {
        let mut metadata_state = self
            .prepare_metadata_state(target.clone(), source_name)
            .await?;

        let push_source = match (&metadata_state.source_event, opts.auto_create_push_source) {
            // No push source, and it's allowed to create
            (None, true) => {
                let add_push_source_event = self
                    .auto_create_push_source(target.clone(), "auto", &opts)
                    .await?;

                // Update data writer, as we've modified the dataset
                metadata_state = self.prepare_metadata_state(target, source_name).await?;
                Ok(add_push_source_event)
            }

            // Got existing push source
            (Some(MetadataEvent::AddPushSource(e)), _) => Ok(e.clone()),

            // No push source and not allowed to create
            _ => Err(PushIngestPlanningError::SourceNotFound(
                PushSourceNotFoundError::new(source_name),
            )),
        }?;

        let operation_id = get_random_name(None, 10);
        let operation_dir = self.run_info_dir.join(format!("ingest-{operation_id}"));

        Ok(PushIngestPlan {
            args: PushIngestArgs {
                operation_id,
                operation_dir,
                system_time: self.time_source.now(),
                opts,
                push_source,
            },
            metadata_state: Box::new(metadata_state),
        })
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn ingest_from_url(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        url: url::Url,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));

        tracing::info!(%url, ?plan, "Ingesting from url");

        self.do_ingest(target, plan, DataSource::Url(url), listener)
            .await
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn ingest_from_file_stream(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        data: Box<dyn AsyncRead + Send + Unpin>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));

        tracing::info!(?plan, "Ingesting from file stream");

        self.do_ingest(target, plan, DataSource::Stream(data), listener)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum DataSource {
    Url(url::Url),
    Stream(Box<dyn AsyncRead + Send + Unpin>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
