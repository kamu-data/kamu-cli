// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;

use file_utils::MediaType;
use kamu::domain;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{ContentArgs, UpdateVersionFileUseCase, UpdateVersionFileUseCaseError};
use tokio::io::BufReader;

use crate::prelude::*;
<<<<<<< HEAD
use crate::queries::{DatasetRequestState, FileVersion};
=======
use crate::queries::{FileVersion, VersionedFileEntry, VersionedFileRecord};
>>>>>>> master

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFileMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

impl<'a> VersionedFileMut<'a> {
<<<<<<< HEAD
    async fn get_content_args(
        &'a self,
        ctx: &Context<'_>,
        content_source: ContentSource<'a>,
        content_type: Option<MediaType>,
    ) -> Result<ContentArgs> {
        use sha3::Digest;
        use tokio::io::AsyncReadExt;

        match content_source {
            ContentSource::Bytes(bytes) => {
                let reader = BufReader::new(Cursor::new(bytes.to_owned()));

                Ok(ContentArgs {
                    content_length: bytes.len(),
                    content_stream: Some(Box::new(reader)),
                    content_hash: odf::Multihash::from_digest_sha3_256(bytes),
                    content_type,
                })
=======
    pub fn new(dataset: &'a domain::ResolvedDataset) -> Self {
        Self { dataset }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_latest_version(&self, ctx: &Context<'_>) -> Result<(FileVersion, odf::Multihash)> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: Consider retractons / corrections
        let query_res = query_svc
            .tail(
                &self.dataset.get_handle().as_local_ref(),
                0,
                1,
                domain::GetDataOptions::default(),
            )
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_latest_entry(&self, ctx: &Context<'_>) -> Result<Option<VersionedFileEntry>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: Consider retractons / corrections
        let query_res = query_svc
            .tail(
                &self.dataset.get_handle().as_local_ref(),
                0,
                1,
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;

        assert_eq!(records.len(), 1);
        let record = records.into_iter().next().unwrap();

        let entry = VersionedFileEntry::from_json(self.dataset.clone(), record)?;

        Ok(Some(entry))
    }

    // Push ingest the new record
    // TODO: Handle errors on invalid extra data columns
    #[tracing::instrument(level = "info", skip_all)]
    async fn write_record(
        &self,
        ctx: &Context<'_>,
        record: VersionedFileRecord,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<UpdateVersionResult> {
        let push_ingest_use_case = from_catalog_n!(ctx, dyn domain::PushIngestDataUseCase);

        let new_version = record.version;
        let content_hash = record.content_hash.clone();
        let record_string = serde_json::to_string(&record).int_err()?;

        tracing::debug!(record = %record_string, "Writing new versioned file record");
        let data_source =
            kamu_core::DataSource::Buffer(bytes::Bytes::from(record_string.into_bytes()));

        let ingest_result = match push_ingest_use_case
            .execute(
                self.dataset,
                data_source,
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(kamu_core::MediaType::NDJSON.to_owned()),
                    expected_head: expected_head.map(Into::into),
                },
                None,
            )
            .await
        {
            Ok(res) => res,
            Err(domain::PushIngestDataError::Execution(domain::PushIngestError::CommitError(
                odf::dataset::CommitError::MetadataAppendError(
                    odf::dataset::AppendError::RefCASFailed(e),
                ),
            ))) => {
                return Ok(UpdateVersionResult::CasFailed(
                    UpdateVersionErrorCasFailed {
                        expected_head: e.expected.unwrap().into(),
                        actual_head: e.actual.unwrap().into(),
                    },
                ));
>>>>>>> master
            }
            ContentSource::Token(token) => {
                let upload_token: domain::UploadTokenBase64Json =
                    token
                        .parse()
                        .map_err(|e: domain::UploadTokenBase64JsonDecodeError| {
                            async_graphql::Error::new(e.message)
                        })?;

<<<<<<< HEAD
                let upload_service = from_catalog_n!(ctx, dyn domain::UploadService);

                let mut stream = upload_service
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

                let content_hash =
                    odf::Multihash::new(odf::metadata::Multicodec::Sha3_256, &digest.finalize())
                        .unwrap();

                // Get the stream again and copy data from uploads to storage using computed
                // hash
                // TODO: PERF: Should we create file in the final storage directly to avoid
                // copying?
                let content_stream = upload_service
                    .upload_token_into_stream(&upload_token.0)
                    .await
                    .int_err()?;

                Ok(ContentArgs {
                    content_length: upload_token.0.content_length,
                    content_hash,
                    content_stream: Some(content_stream),
                    content_type: upload_token.0.content_type,
                })
            }
=======
        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
                new_version,
                old_head: old_head.into(),
                new_head: new_head.into(),
                content_hash: content_hash.into(),
            })),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
>>>>>>> master
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> VersionedFileMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Uploads a new version of content in-band. Can be used for very small
    /// files only.
    #[tracing::instrument(level = "info", name = VersionedFileMut_upload_new_version, skip_all)]
    #[graphql(guard = "LoggedInGuard")]
    pub async fn upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Base64-encoded file content (url-safe, no padding)")] content: Base64Usnp,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
        #[graphql(desc = "Json object containing values of extra columns")] extra_data: Option<
            ExtraData,
        >,
        #[graphql(desc = "Expected head block hash to prevent concurrent updates")]
        expected_head: Option<Multihash<'static>>,
    ) -> Result<UpdateVersionResult> {
        let update_version_file_use_case = from_catalog_n!(ctx, dyn UpdateVersionFileUseCase);

        let content_args = self
            .get_content_args(
                ctx,
                ContentSource::Bytes(&content),
                content_type.map(Into::into),
            )
            .await?;

        match update_version_file_use_case
            .execute(
                self.dataset_request_state.dataset_handle(),
                Some(content_args),
                expected_head.map(Into::into),
                extra_data.map(Into::into),
            )
            .await
<<<<<<< HEAD
        {
            Ok(res) => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
                new_version: res.new_version,
                old_head: res.old_head.into(),
                new_head: res.new_head.into(),
                content_hash: res.content_hash.into(),
            })),
            Err(UpdateVersionFileUseCaseError::TooLarge(err)) => {
                Ok(UpdateVersionResult::TooLarge(UploadVersionErrorTooLarge {
                    upload_size: err.upload_size,
                    upload_limit: err.upload_limit,
                }))
            }
            Err(UpdateVersionFileUseCaseError::RefCASFailed(err)) => {
                return Ok(UpdateVersionResult::CasFailed(
                    UpdateVersionErrorCasFailed {
                        expected_head: err.expected.unwrap().into(),
                        actual_head: err.actual.unwrap().into(),
                    },
                ));
            }
            Err(err) => {
                return Err(err.int_err().into());
            }
        }
=======
            .int_err()?;
        let content_hash = insert_data_res.hash;

        // Form and write a new record
        let entry = VersionedFileEntry::new(
            self.dataset.clone(),
            new_version,
            content_hash.clone(),
            content_length,
            content_type,
            extra_data,
        );

        self.write_record(ctx, entry.into_input_record(), expected_head)
            .await
>>>>>>> master
    }

    /// Returns a pre-signed URL and upload token for direct uploads of large
    /// files
    #[tracing::instrument(level = "info", name = VersionedFileMut_start_upload_new_version, skip_all)]
    #[graphql(guard = "LoggedInGuard")]
    pub async fn start_upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Size of the file being uploaded")] content_length: usize,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
    ) -> Result<StartUploadVersionResult> {
        let (subject, upload_svc, limits) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn domain::UploadService,
            domain::FileUploadLimitConfig
        );

        let upload_context = match upload_svc
            .make_upload_context(
                subject.account_id(),
                uuid::Uuid::new_v4().to_string(),
                content_type.map(MediaType),
                content_length,
            )
            .await
        {
            Ok(ctx) => ctx,
            Err(domain::MakeUploadContextError::TooLarge(_)) => {
                return Ok(StartUploadVersionResult::TooLarge(
                    UploadVersionErrorTooLarge {
                        upload_size: content_length,
                        upload_limit: limits.max_file_size_in_bytes(),
                    },
                ));
            }
            Err(e) => return Err(e.int_err().into()),
        };

        Ok(StartUploadVersionResult::Success(
            StartUploadVersionSuccess {
                upload_context: upload_context.into(),
            },
        ))
    }

    /// Finalizes the content upload by incorporating the content into the
    /// dataset as a new version
    #[tracing::instrument(level = "info", name = VersionedFileMut_finish_upload_new_version, skip_all)]
    #[graphql(guard = "LoggedInGuard")]
    pub async fn finish_upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Token received when starting the upload")] upload_token: String,
        #[graphql(desc = "Json object containing values of extra columns")] extra_data: Option<
            ExtraData,
        >,
        #[graphql(desc = "Expected head block hash to prevent concurrent updates")]
        expected_head: Option<Multihash<'static>>,
    ) -> Result<UpdateVersionResult> {
        let update_version_file_use_case = from_catalog_n!(ctx, dyn UpdateVersionFileUseCase);

        let content_args = self
            .get_content_args(ctx, ContentSource::Token(upload_token), None)
            .await?;

        match update_version_file_use_case
            .execute(
                self.dataset_request_state.dataset_handle(),
                Some(content_args),
                expected_head.map(Into::into),
                extra_data.map(Into::into),
            )
            .await
<<<<<<< HEAD
        {
            Ok(res) => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
                new_version: res.new_version,
                old_head: res.old_head.into(),
                new_head: res.new_head.into(),
                content_hash: res.content_hash.into(),
            })),
            Err(UpdateVersionFileUseCaseError::TooLarge(err)) => {
                Ok(UpdateVersionResult::TooLarge(UploadVersionErrorTooLarge {
                    upload_size: err.upload_size,
                    upload_limit: err.upload_limit,
                }))
            }
            Err(UpdateVersionFileUseCaseError::RefCASFailed(err)) => {
                return Ok(UpdateVersionResult::CasFailed(
                    UpdateVersionErrorCasFailed {
                        expected_head: err.expected.unwrap().into(),
                        actual_head: err.actual.unwrap().into(),
                    },
                ));
            }
            Err(err) => {
                return Err(err.int_err().into());
            }
        }
=======
            .int_err()?;

        // Form and write a new record
        let entry = VersionedFileEntry::new(
            self.dataset.clone(),
            new_version,
            content_hash.clone(),
            content_length,
            upload_token.0.content_type,
            extra_data,
        );

        self.write_record(ctx, entry.into_input_record(), expected_head)
            .await
>>>>>>> master
    }

    /// Creating a new version with that has updated values of extra columns but
    /// with the file content unchanged
    #[tracing::instrument(level = "info", name = VersionedFileMut_update_extra_data, skip_all)]
    #[graphql(guard = "LoggedInGuard")]
    pub async fn update_extra_data(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Json object containing values of extra columns")] extra_data: ExtraData,
        #[graphql(desc = "Expected head block hash to prevent concurrent updates")]
        expected_head: Option<Multihash<'static>>,
    ) -> Result<UpdateVersionResult> {
        let update_version_file_use_case = from_catalog_n!(ctx, dyn UpdateVersionFileUseCase);

<<<<<<< HEAD
        match update_version_file_use_case
            .execute(
                self.dataset_request_state.dataset_handle(),
                None,
                expected_head.map(Into::into),
                Some(extra_data.into()),
            )
            .await
        {
            Ok(res) => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
                new_version: res.new_version,
                old_head: res.old_head.into(),
                new_head: res.new_head.into(),
                content_hash: res.content_hash.into(),
            })),
            Err(UpdateVersionFileUseCaseError::TooLarge(err)) => {
                Ok(UpdateVersionResult::TooLarge(UploadVersionErrorTooLarge {
                    upload_size: err.upload_size,
                    upload_limit: err.upload_limit,
                }))
            }
            Err(UpdateVersionFileUseCaseError::RefCASFailed(err)) => {
                return Ok(UpdateVersionResult::CasFailed(
                    UpdateVersionErrorCasFailed {
                        expected_head: err.expected.unwrap().into(),
                        actual_head: err.actual.unwrap().into(),
                    },
                ));
            }
            Err(err) => {
                return Err(err.int_err().into());
            }
        }
=======
        // Form and write a new record
        entry.version += 1;
        entry.extra_data = extra_data;

        self.write_record(ctx, entry.into_input_record(), expected_head)
            .await
>>>>>>> master
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum UpdateVersionResult {
    Success(UpdateVersionSuccess),
    CasFailed(UpdateVersionErrorCasFailed),
    InvalidExtraData(UpdateVersionErrorInvalidExtraData),
    TooLarge(UploadVersionErrorTooLarge),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionSuccess {
    pub new_version: FileVersion,
    pub old_head: Multihash<'static>,
    pub new_head: Multihash<'static>,
    pub content_hash: Multihash<'static>,
}
#[ComplexObject]
impl UpdateVersionSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionErrorCasFailed {
    expected_head: Multihash<'static>,
    actual_head: Multihash<'static>,
}
#[ComplexObject]
impl UpdateVersionErrorCasFailed {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        "Expected head didn't match, dataset was likely updated concurrently".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionErrorInvalidExtraData {
    message: String,
}
#[ComplexObject]
impl UpdateVersionErrorInvalidExtraData {
    async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum StartUploadVersionResult {
    Success(StartUploadVersionSuccess),
    TooLarge(UploadVersionErrorTooLarge),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct StartUploadVersionSuccess {
    #[graphql(flatten)]
    upload_context: UploadContext,
}
#[ComplexObject]
impl StartUploadVersionSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UploadVersionErrorTooLarge {
    upload_size: usize,
    upload_limit: usize,
}
#[ComplexObject]
impl UploadVersionErrorTooLarge {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        format!(
            "Upload of {} exceeds the {} limit",
            humansize::format_size(self.upload_size, humansize::BINARY),
            humansize::format_size(self.upload_limit, humansize::BINARY)
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
pub struct UploadContext {
    pub url: String,
    pub method: String,
    pub use_multipart: bool,
    pub headers: Vec<KeyValue>,
    pub upload_token: String,
}

impl From<domain::UploadContext> for UploadContext {
    fn from(value: domain::UploadContext) -> Self {
        assert_eq!(value.fields, Vec::new());

        Self {
            url: value.upload_url,
            method: value.method,
            use_multipart: value.use_multipart,
            headers: value
                .headers
                .into_iter()
                .map(|(key, value)| KeyValue { key, value })
                .collect(),
            upload_token: value.upload_token.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum ContentSource<'a> {
    Bytes(&'a [u8]),
    Token(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
