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
use dill::*;
use kamu_core::ingest::*;
use kamu_core::*;
use kamu_ingest_datafusion::*;
use opendatafabric::*;
use tokio::io::AsyncRead;

use super::ingest_common;

///////////////////////////////////////////////////////////////////////////////

pub struct PushIngestServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    data_format_registry: Arc<dyn DataFormatRegistry>,
    run_info_dir: PathBuf,
    time_source: Arc<dyn SystemTimeSource>,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl PushIngestServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        data_format_registry: Arc<dyn DataFormatRegistry>,
        run_info_dir: PathBuf,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            object_store_registry,
            data_format_registry,
            run_info_dir,
            time_source,
        }
    }

    async fn do_ingest(
        &self,
        dataset_ref: &DatasetRef,
        source_name: Option<&str>,
        source: DataSource,
        media_type: Option<MediaType>,
        listener: Arc<dyn PushIngestListener>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(&dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Write)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let operation_id = ingest_common::next_operation_id();
        let operation_dir = self.run_info_dir.join(format!("ingest-{}", operation_id));
        std::fs::create_dir_all(&operation_dir).int_err()?;

        let ctx: SessionContext =
            ingest_common::new_session_context(self.object_store_registry.clone());

        let data_writer = match DataWriterDataFusion::builder(dataset.clone(), ctx.clone())
            .with_metadata_state_scanned(source_name)
            .await
        {
            Ok(b) => Ok(b.build()),
            Err(ScanMetadataError::SourceNotFound(err)) => {
                Err(PushIngestError::SourceNotFound(err.into()))
            }
            Err(ScanMetadataError::Internal(err)) => Err(PushIngestError::Internal(err)),
        }?;

        let push_source = match data_writer.source_event() {
            Some(MetadataEvent::AddPushSource(e)) => Ok(e.clone()),
            _ => Err(PushIngestError::SourceNotFound(
                PushSourceNotFoundError::new(source_name),
            )),
        }?;

        let args = PushIngestArgs {
            operation_id,
            operation_dir,
            system_time: self.time_source.now(),
            media_type,
            listener,
            ctx,
            data_writer,
            push_source,
        };

        let listener = args.listener.clone();
        listener.begin();

        match self.do_ingest_inner(source, args).await {
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

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            operation_id = %args.operation_id,
        )
    )]
    async fn do_ingest_inner(
        &self,
        source: DataSource,
        mut args: PushIngestArgs,
    ) -> Result<PushIngestResult, PushIngestError> {
        args.listener
            .on_stage_progress(PushIngestStage::Read, 0, TotalSteps::Exact(1));

        let input_data_path = self.maybe_fetch(source, &args).await?;

        let df = if let Some(df) = self.read(&input_data_path, &args).await? {
            if let Some(transform) = args.push_source.preprocess.clone() {
                Some(ingest_common::preprocess(&args.ctx, transform, df).await?)
            } else {
                Some(df)
            }
        } else {
            tracing::info!("Read produced an empty data frame");
            None
        };

        let out_dir = args.operation_dir.join("out");
        let data_staging_path = out_dir.join("data");
        std::fs::create_dir(&out_dir).int_err()?;

        let stage_result = args
            .data_writer
            .stage(
                df,
                WriteDataOpts {
                    system_time: args.system_time.clone(),
                    source_event_time: args.system_time.clone(),
                    source_state: None, // TODO: Support storing ingest source state
                    data_staging_path,
                },
            )
            .await;

        match stage_result {
            Ok(staged) => {
                args.listener
                    .on_stage_progress(PushIngestStage::Commit, 0, TotalSteps::Exact(1));

                let res = args.data_writer.commit(staged).await?;

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
                            .map_err(|_| format!("Invalid file URL {}", url).int_err())?;

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
                    _ => Err(format!("Unsupported source: {}", url).int_err().into()),
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
        args: &PushIngestArgs,
    ) -> Result<Option<DataFrame>, PushIngestError> {
        if input_data_path.metadata().int_err()?.len() == 0 {
            tracing::info!(path = ?input_data_path, "Early return due to an empty
        file");
            return Ok(None);
        }

        let conf = if let Some(media_type) = &args.media_type {
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
            .get_reader(args.ctx.clone(), conf, temp_path)
            .await?;

        let df = reader.read(&input_data_path).await?;

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

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PushIngestService for PushIngestServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_active_push_sources(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Vec<(Multihash, MetadataBlockTyped<AddPushSource>)>, GetDatasetError> {
        use futures::TryStreamExt;

        // TODO: Support source disabling and evolution
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await?;
        let stream = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(h, b)| b.into_typed().map(|b| (h, b)));

        Ok(stream.try_collect().await.int_err()?)
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, %url, ?media_type))]
    async fn ingest_from_url(
        &self,
        dataset_ref: &DatasetRef,
        source_name: Option<&str>,
        url: url::Url,
        media_type: Option<MediaType>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));

        self.do_ingest(
            dataset_ref,
            source_name,
            DataSource::Url(url),
            media_type,
            listener,
        )
        .await
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, ?media_type))]
    async fn ingest_from_file_stream(
        &self,
        dataset_ref: &DatasetRef,
        source_name: Option<&str>,
        data: Box<dyn AsyncRead + Send + Unpin>,
        media_type: Option<MediaType>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));

        self.do_ingest(
            dataset_ref,
            source_name,
            DataSource::Stream(data),
            media_type,
            listener,
        )
        .await
    }
}

struct PushIngestArgs {
    operation_id: String,
    operation_dir: PathBuf,
    system_time: DateTime<Utc>,
    media_type: Option<MediaType>,
    listener: Arc<dyn PushIngestListener>,
    ctx: SessionContext,
    data_writer: DataWriterDataFusion,
    push_source: AddPushSource,
}

enum DataSource {
    Url(url::Url),
    Stream(Box<dyn AsyncRead + Send + Unpin>),
}
