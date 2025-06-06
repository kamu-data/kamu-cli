// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;
use std::sync::Arc;

use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{
    ContentSource,
    DatasetRegistry,
    ExtraDataFields,
    FileUploadLimitConfig,
    FileVersion,
    GetDataOptions,
    MediaType,
    PushIngestDataUseCase,
    QueryService,
    UpdateVersionFileResult,
    UpdateVersionFileUseCase,
    UpdateVersionFileUseCaseError,
    UploadService,
    UploadTokenBase64Json,
    UploadTooLargeError,
    VersionedFileEntity,
};
use sha3::Digest;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn UpdateVersionFileUseCase)]
pub struct UpdateVersionFileUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    upload_config: Arc<FileUploadLimitConfig>,
    upload_service: Arc<dyn UploadService>,
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
}

impl UpdateVersionFileUseCaseImpl {
    async fn get_latest_version(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<(FileVersion, odf::Multihash), InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(dataset_ref, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok((0, query_res.block_hash));
        };

        let last_version = df
            .select_columns(&["version"])
            .int_err()?
            .collect_scalar::<datafusion::arrow::datatypes::Int32Type>()
            .await
            .int_err()?;

        let last_version = FileVersion::try_from(last_version.unwrap_or(0)).unwrap();

        Ok((last_version, query_res.block_hash))
    }

    async fn get_content_info_from_latest_entry(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<ContentInfo>, InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(dataset_ref, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;

        assert_eq!(records.len(), 1);
        let serde_json::Value::Object(mut record) = records.into_iter().next().unwrap() else {
            unreachable!()
        };

        // TODO: Restrict after migration
        let content_length = usize::try_from(
            record
                .remove("content_length")
                .unwrap_or_default()
                .as_u64()
                .unwrap_or_default(),
        )
        .unwrap();
        let content_type =
            MediaType::from(record.remove("content_type").unwrap().as_str().unwrap());
        let content_hash = odf::Multihash::from_multibase(
            record.remove("content_hash").unwrap().as_str().unwrap(),
        )
        .unwrap();

        Ok(Some(ContentInfo {
            content_length,
            content_stream: None,
            content_hash,
            content_type: Some(content_type),
        }))
    }

    async fn content_source_to_content_info<'a>(
        &'a self,
        content_source: ContentSource<'a>,
        content_type: Option<MediaType>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ContentInfo, InternalError> {
        match content_source {
            ContentSource::Empty => Ok(self
                .get_content_info_from_latest_entry(dataset_ref)
                .await?
                .unwrap()),
            ContentSource::Bytes(bytes) => {
                let reader = BufReader::new(Cursor::new(bytes.to_owned()));

                Ok(ContentInfo {
                    content_length: bytes.len(),
                    content_stream: Some(Box::new(reader)),
                    content_hash: odf::Multihash::from_digest_sha3_256(bytes),
                    content_type,
                })
            }
            ContentSource::Token(token) => {
                // ToDo add error
                let upload_token: UploadTokenBase64Json = token.parse().unwrap();

                let mut stream = self
                    .upload_service
                    .upload_token_into_stream(&upload_token.0)
                    .await
                    .int_err()?;

                let mut digest = sha3::Sha3_256::new();
                let mut buf = [0u8; 2048];

                loop {
                    let read = stream.read(&mut buf).await.int_err()?;
                    if read == 0 {
                        break;
                    }
                    digest.update(&buf[..read]);
                }

                let digest = digest.finalize();
                let content_hash =
                    odf::Multihash::new(odf::metadata::Multicodec::Sha3_256, &digest).unwrap();

                // Get the stream again and copy data from uploads to storage using computed
                // hash
                // TODO: PERF: Should we create file in the final storage directly to avoid
                // copying?
                let content_stream = self
                    .upload_service
                    .upload_token_into_stream(&upload_token.0)
                    .await
                    .int_err()?;

                Ok(ContentInfo {
                    content_length: upload_token.0.content_length,
                    content_hash,
                    content_stream: Some(content_stream),
                    content_type: upload_token.0.content_type,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateVersionFileUseCase for UpdateVersionFileUseCaseImpl {
    // ToDo Add fields
    #[tracing::instrument(level = "info", name = UpdateVersionFileUseCaseImpl_execute, skip_all)]
    async fn execute<'a>(
        &'a self,
        dataset_handle: &'a odf::DatasetHandle,
        content_source: ContentSource<'a>,
        content_type: Option<MediaType>,
        expected_head: Option<odf::Multihash>,
        extra_data: Option<ExtraDataFields>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError> {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await
            .clone();

        let content_info = self
            .content_source_to_content_info(
                content_source,
                content_type,
                &resolved_dataset.get_handle().as_local_ref(),
            )
            .await
            .int_err()?;

        if content_info.content_length > self.upload_config.max_file_size_in_bytes() {
            return Err(UpdateVersionFileUseCaseError::TooLarge(
                UploadTooLargeError::new(
                    content_info.content_length,
                    self.upload_config.max_file_size_in_bytes(),
                ),
            ));
        }

        // Upload data object
        if let Some(content_stream) = content_info.content_stream {
            let data_repo = resolved_dataset.as_data_repo();
            data_repo
                .insert_stream(
                    content_stream,
                    odf::storage::InsertOpts {
                        precomputed_hash: Some(&content_info.content_hash),
                        size_hint: Some(content_info.content_length as u64),
                        ..Default::default()
                    },
                )
                .await
                .int_err()?;
        }

        let (latest_version, _) = self
            .get_latest_version(&resolved_dataset.get_handle().as_local_ref())
            .await?;
        let new_version = latest_version + 1;

        let entity = VersionedFileEntity::new(
            new_version,
            content_info.content_hash.clone(),
            content_info.content_length,
            content_info.content_type,
            extra_data,
        );

        let ingest_result = self
            .push_ingest_data_use_case
            .execute(
                &resolved_dataset,
                kamu_core::DataSource::Buffer(entity.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(kamu_core::MediaType::NDJSON.to_owned()),
                    expected_head,
                },
                None,
            )
            .await?;

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(UpdateVersionFileResult {
                new_version,
                old_head,
                new_head,
                content_hash: content_info.content_hash,
            }),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ContentInfo {
    pub content_length: usize,
    pub content_hash: odf::Multihash,
    pub content_stream: Option<Box<dyn AsyncRead + Send + Unpin>>,
    pub content_type: Option<MediaType>,
}
