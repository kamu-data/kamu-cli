// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use file_utils::MediaType;
use kamu::domain;
use kamu_accounts::{CurrentAccountSubject, QuotaError};
use kamu_datasets::{
    UpdateVersionFileResult,
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
    WriteCheckedDataset,
};

use crate::prelude::*;
use crate::queries::{DatasetRequestState, FileVersion};
use crate::utils::{ContentSource, GetContentArgsError, get_content_args};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFileMut<'a> {
    writable_state: &'a DatasetRequestState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mutations of a versioned file dataset
#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> VersionedFileMut<'a> {
    #[graphql(skip)]
    pub fn new(writable_state: &'a DatasetRequestState) -> Self {
        Self { writable_state }
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
        let update_versioned_file = from_catalog_n!(ctx, dyn UpdateVersionedFileUseCase);

        let content_args = get_content_args(
            ctx,
            ContentSource::Bytes(&content),
            content_type.map(Into::into),
        )
        .await
        .map_err(map_get_content_args_error)?;

        let file_dataset = self.writable_state.resolved_dataset(ctx).await?;

        let result = update_versioned_file
            .execute(
                WriteCheckedDataset::from_ref(file_dataset),
                None,
                Some(content_args),
                expected_head.map(Into::into),
                extra_data.map(Into::into),
            )
            .await;

        update_version_result(result)
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
                    StartUploadVersionErrorTooLarge {
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
        let update_versioned_file = from_catalog_n!(ctx, dyn UpdateVersionedFileUseCase);

        let content_args = get_content_args(ctx, ContentSource::Token(upload_token), None)
            .await
            .map_err(map_get_content_args_error)?;

        let file_dataset = self.writable_state.resolved_dataset(ctx).await?;

        let result = update_versioned_file
            .execute(
                WriteCheckedDataset::from_ref(file_dataset),
                None,
                Some(content_args),
                expected_head.map(Into::into),
                extra_data.map(Into::into),
            )
            .await;

        update_version_result(result)
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
        let update_versioned_file = from_catalog_n!(ctx, dyn UpdateVersionedFileUseCase);

        let file_dataset = self.writable_state.resolved_dataset(ctx).await?;

        let result = update_versioned_file
            .execute(
                WriteCheckedDataset::from_ref(file_dataset),
                None,
                None,
                expected_head.map(Into::into),
                Some(extra_data.into()),
            )
            .await;

        update_version_result(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of creating a new file version
#[derive(Interface)]
#[graphql(
    field(
        name = "is_success",
        ty = "bool",
        desc = "Whether the operation succeeded"
    ),
    field(
        name = "message",
        ty = "String",
        desc = "Human-readable outcome message"
    )
)]
pub enum UpdateVersionResult {
    Success(UpdateVersionSuccess),
    CasFailed(UpdateVersionErrorCasFailed),
    InvalidExtraData(UpdateVersionErrorInvalidExtraData),
    NoVersions(UpdateVersionErrorNoVersions),
    QuotaExceeded(UpdateVersionErrorQuotaExceeded),
}

/// New file version was created
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionSuccess {
    /// Number of the created version
    pub new_version: FileVersion,
    /// Hash of the metadata chain head before the update
    pub old_head: Multihash<'static>,
    /// Hash of the metadata chain head after the update
    pub new_head: Multihash<'static>,
    /// Hash of the file content of the new version
    pub content_hash: Multihash<'static>,
}
#[ComplexObject]
impl UpdateVersionSuccess {
    /// Always true
    pub async fn is_success(&self) -> bool {
        true
    }
    /// Always empty
    pub async fn message(&self) -> String {
        String::new()
    }
}

/// Dataset head did not match the expected one, likely due to a concurrent update
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionErrorCasFailed {
    /// Head block hash the caller expected
    pub expected_head: Multihash<'static>,
    /// Current head block hash of the dataset
    pub actual_head: Multihash<'static>,
}
#[ComplexObject]
impl UpdateVersionErrorCasFailed {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    pub async fn message(&self) -> String {
        "Expected head didn't match, dataset was likely updated concurrently".to_string()
    }
}

/// Extra data does not match the dataset schema
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionErrorInvalidExtraData {
    /// Reason the extra data was rejected
    pub message: String,
}
#[ComplexObject]
impl UpdateVersionErrorInvalidExtraData {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
}

pub struct UpdateVersionErrorNoVersions;

/// Extra data cannot be updated, as the file has no versions yet
#[Object]
impl UpdateVersionErrorNoVersions {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    pub async fn message(&self) -> String {
        "File has no versions to update extra data of".to_string()
    }
}

/// Update would exceed the storage quota of the account
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionErrorQuotaExceeded {
    /// Storage already used, in bytes; null if no quota is configured
    pub used: Option<u64>,
    /// Size of the rejected content, in bytes; null if no quota is configured
    pub incoming: Option<u64>,
    /// Storage limit, in bytes; null if no quota is configured
    pub limit: Option<u64>,
}

#[ComplexObject]
impl UpdateVersionErrorQuotaExceeded {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }

    /// Human-readable error message
    pub async fn message(&self) -> String {
        match (self.used, self.incoming, self.limit) {
            (Some(used), Some(incoming), Some(limit)) => {
                format!("Quota exceeded: used={used}, incoming={incoming}, limit={limit}")
            }
            _ => "Quota exceeded".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of starting a direct file upload
#[derive(Interface)]
#[graphql(
    field(
        name = "is_success",
        ty = "bool",
        desc = "Whether the operation succeeded"
    ),
    field(
        name = "message",
        ty = "String",
        desc = "Human-readable outcome message"
    )
)]
pub enum StartUploadVersionResult {
    Success(StartUploadVersionSuccess),
    TooLarge(StartUploadVersionErrorTooLarge),
}

/// Upload was started; describes how to send the content
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct StartUploadVersionSuccess {
    #[graphql(flatten)]
    pub upload_context: UploadContext,
}
#[ComplexObject]
impl StartUploadVersionSuccess {
    /// Always true
    async fn is_success(&self) -> bool {
        true
    }
    /// Always empty
    async fn message(&self) -> String {
        String::new()
    }
}

/// File is larger than the upload size limit
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct StartUploadVersionErrorTooLarge {
    /// Size of the file, in bytes
    pub upload_size: usize,
    /// Maximum allowed upload size, in bytes
    pub upload_limit: usize,
}
#[ComplexObject]
impl StartUploadVersionErrorTooLarge {
    /// Always false
    async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
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
    /// URL to send the file content to
    pub url: String,
    /// HTTP method to use for the upload
    pub method: String,
    /// Whether the content must be sent as a multipart form
    pub use_multipart: bool,
    /// HTTP headers to include in the upload request
    pub headers: Vec<KeyValue>,
    /// Token to pass to `finishUploadNewVersion` once the upload completes
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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn map_get_content_args_error(e: GetContentArgsError) -> GqlError {
    match e {
        GetContentArgsError::TokenDecode(e) => GqlError::gql(e.message),
        e @ GetContentArgsError::Internal(_) => e.int_err().into(),
    }
}

fn update_version_result(
    result: Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError>,
) -> Result<UpdateVersionResult> {
    match result {
        Ok(res) => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
            new_version: res.new_version,
            old_head: res.old_head.into(),
            new_head: res.new_head.into(),
            content_hash: res.content_hash.into(),
        })),
        Err(UpdateVersionFileUseCaseError::RefCASFailed(err)) => Ok(
            UpdateVersionResult::CasFailed(UpdateVersionErrorCasFailed {
                expected_head: err.expected.unwrap().into(),
                actual_head: err.actual.unwrap().into(),
            }),
        ),
        Err(UpdateVersionFileUseCaseError::NoVersions) => Ok(UpdateVersionResult::NoVersions(
            UpdateVersionErrorNoVersions,
        )),
        Err(UpdateVersionFileUseCaseError::QuotaExceeded(err)) => match err {
            QuotaError::Exceeded(e) => Ok(UpdateVersionResult::QuotaExceeded(
                UpdateVersionErrorQuotaExceeded {
                    used: Some(e.used),
                    incoming: Some(e.incoming),
                    limit: Some(e.limit),
                },
            )),
            QuotaError::NotConfigured => Ok(UpdateVersionResult::QuotaExceeded(
                UpdateVersionErrorQuotaExceeded {
                    used: None,
                    incoming: None,
                    limit: None,
                },
            )),
            QuotaError::Internal(e) => Err(e.into()),
        },
        Err(
            e @ (UpdateVersionFileUseCaseError::Access(_)
            | UpdateVersionFileUseCaseError::Internal(_)),
        ) => Err(e.int_err().into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
