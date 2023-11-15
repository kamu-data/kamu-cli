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
    run_info_dir: PathBuf,
    cache_dir: PathBuf,
    time_source: Arc<dyn SystemTimeSource>,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl PushIngestServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        run_info_dir: PathBuf,
        cache_dir: PathBuf,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            object_store_registry,
            run_info_dir,
            cache_dir,
            time_source,
        }
    }

    async fn do_ingest(
        &self,
        dataset_ref: &DatasetRef,
        url: url::Url,
        media_type: Option<&str>,
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

        // TODO: Temporarily relying on SetPollingSource event
        let Some(polling_source) = dataset
            .as_metadata_chain()
            .last_of_type::<SetPollingSource>()
            .await
            .int_err()?
            .map(|(_, b)| b.event)
        else {
            let err = PushIngestError::SourceNotFound(PushSourceNotFoundError);
            listener.begin();
            listener.error(&err);
            return Err(err);
        };

        let operation_id = ingest_common::next_operation_id();
        let operation_dir = self.run_info_dir.join(format!("ingest-{}", operation_id));
        std::fs::create_dir_all(&operation_dir).int_err()?;

        let ctx: SessionContext =
            ingest_common::new_session_context(self.object_store_registry.clone());
        let data_writer = DataWriterDataFusion::builder(dataset.clone(), ctx.clone())
            .with_metadata_state_scanned()
            .await?
            .build()
            .await?;

        let args = PushIngestArgs {
            operation_id,
            operation_dir,
            system_time: self.time_source.now(),
            url,
            media_type: media_type.map(|s| s.to_string()),
            listener,
            ctx,
            data_writer,
            polling_source,
        };

        let listener = args.listener.clone();
        listener.begin();

        match self.do_ingest_inner(args).await {
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
        mut args: PushIngestArgs,
    ) -> Result<PushIngestResult, PushIngestError> {
        args.listener
            .on_stage_progress(PushIngestStage::CheckSource, 0, TotalSteps::Exact(1));
        args.listener
            .on_stage_progress(PushIngestStage::Fetch, 0, TotalSteps::Exact(1));
        args.listener
            .on_stage_progress(PushIngestStage::Read, 0, TotalSteps::Exact(1));

        let df = if let Some(df) = self.read(&args).await? {
            if let Some(transform) = args.polling_source.preprocess.clone() {
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
                    source_state: None,
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
            Err(StageDataError::MergeError(e)) => Err(e.into()),
            Err(StageDataError::EmptyCommit(_)) => Ok(PushIngestResult::UpToDate),
            Err(StageDataError::Internal(e)) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn read(&self, args: &PushIngestArgs) -> Result<Option<DataFrame>, PushIngestError> {
        // TODO: Support S3
        let input_data_path: PathBuf = match args.url.scheme() {
            "file" => {
                let p = args
                    .url
                    .to_file_path()
                    .map_err(|_| format!("Invalid file URL {}", args.url).int_err())?;

                // TODO: In case of STDIN (/dev/fd/0) or other pipes and special device files
                // we have to copy data into a temporary file, as DataFusion cannot read from
                // them directly.
                cfg_if::cfg_if! {
                    if #[cfg(unix)] {
                        use std::os::unix::fs::FileTypeExt;
                        let ft = p.metadata().int_err()?.file_type();
                        if ft.is_fifo() || ft.is_char_device() || ft.is_block_device() {
                            let temp_path = args.operation_dir.join(ingest_common::get_random_cache_key("read-"));
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
            _ => Err(format!("Unsupported source: {}", args.url).int_err()),
        }?;

        if input_data_path.metadata().int_err()?.len() == 0 {
            tracing::info!(path = ?input_data_path, "Early return due to an empty file");
            return Ok(None);
        }

        let read_step = if let Some(media_type) = &args.media_type {
            if Self::media_type_for(&args.polling_source.read) == media_type {
                // Use the read format from source
                args.polling_source.read.clone()
            } else {
                // Pushing with different format than the source - will have to perform
                // best-effort conversion
                Self::get_read_step_for(media_type, &args.polling_source.read)?
            }
        } else {
            // Use the read format from source
            args.polling_source.read.clone()
        };

        let reader = Self::get_reader_for(&read_step, &args.operation_dir);
        let df = reader.read(&args.ctx, &input_data_path, &read_step).await?;

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
            use std::io::{Read, Write};
            let mut buf = [0u8; 2048];
            loop {
                let read = source.read(&mut buf)?;
                if read == 0 {
                    break;
                }
                target.write_all(&buf[..read])?;
            }
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut file = tokio::fs::File::create(target_path).await?;
        let mut buf = [0u8; 2048];
        loop {
            let read = data.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            file.write_all(&buf[..read]).await?;
        }
        file.flush().await?;
        Ok(())
    }

    fn media_type_for(source_read_step: &ReadStep) -> &'static str {
        match source_read_step {
            ReadStep::Csv(_) => IngestMediaTypes::CSV,
            ReadStep::Json(_) => IngestMediaTypes::JSON,
            ReadStep::NdJson(_) => IngestMediaTypes::NDJSON,
            ReadStep::JsonLines(_) => IngestMediaTypes::NDJSON,
            ReadStep::GeoJson(_) => IngestMediaTypes::GEOJSON,
            ReadStep::NdGeoJson(_) => IngestMediaTypes::NDGEOJSON,
            ReadStep::Parquet(_) => IngestMediaTypes::PARQUET,
            ReadStep::EsriShapefile(_) => IngestMediaTypes::ESRISHAPEFILE,
        }
    }

    fn get_read_step_for(
        media_type: &str,
        source_read_step: &ReadStep,
    ) -> Result<ReadStep, PushIngestError> {
        let schema = source_read_step.schema().cloned();
        match media_type {
            IngestMediaTypes::CSV => Ok(ReadStepCsv {
                schema,
                ..Default::default()
            }
            .into()),
            IngestMediaTypes::JSON => Ok(ReadStepJson {
                schema,
                ..Default::default()
            }
            .into()),
            IngestMediaTypes::NDJSON => Ok(ReadStepNdJson {
                schema,
                ..Default::default()
            }
            .into()),
            IngestMediaTypes::GEOJSON => Ok(ReadStepGeoJson { schema }.into()),
            IngestMediaTypes::NDGEOJSON => Ok(ReadStepNdGeoJson { schema }.into()),
            IngestMediaTypes::PARQUET => Ok(ReadStepParquet { schema }.into()),
            IngestMediaTypes::ESRISHAPEFILE => Ok(ReadStepEsriShapefile {
                schema,
                ..Default::default()
            }
            .into()),
            _ => Err(UnsupportedMediaTypeError::new(media_type).into()),
        }
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
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PushIngestService for PushIngestServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, %url, ?media_type))]
    async fn ingest_from_url(
        &self,
        dataset_ref: &DatasetRef,
        url: url::Url,
        media_type: Option<&str>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));

        self.do_ingest(dataset_ref, url, media_type, listener).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, ?media_type))]
    async fn ingest_from_file_stream(
        &self,
        dataset_ref: &DatasetRef,
        data: Box<dyn AsyncRead + Send + Unpin>,
        media_type: Option<&str>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        // Save stream to a file in cache
        //
        // TODO: Breaking all architecture layers here - need to extract cache into a
        // service
        let path = self
            .cache_dir
            .join(ingest_common::get_random_cache_key("push-ingest-"));
        Self::copy_stream_to_file(data, &path).await.int_err()?;

        let listener = listener.unwrap_or_else(|| Arc::new(NullPushIngestListener));
        let url: url::Url = url::Url::from_file_path(&path).unwrap();

        let res = self.do_ingest(dataset_ref, url, media_type, listener).await;

        // Clean up the file in cache as it's non-reusable
        std::fs::remove_file(path).int_err()?;

        res
    }
}

struct PushIngestArgs {
    operation_id: String,
    operation_dir: PathBuf,
    system_time: DateTime<Utc>,
    url: url::Url,
    media_type: Option<String>,
    listener: Arc<dyn PushIngestListener>,
    ctx: SessionContext,
    data_writer: DataWriterDataFusion,
    polling_source: SetPollingSource,
}
