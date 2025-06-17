// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use odf::dataset::RefCASError;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::{ExtraDataFields, FileVersion, MediaType, PushIngestDataError, PushIngestError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateVersionFileUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        content_info_maybe: Option<ContentInfo>,
        expected_head: Option<odf::Multihash>,
        extra_data: Option<ExtraDataFields>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ContentSource<'a> {
    Bytes(&'a [u8]),
    Token(String),
    Empty,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ContentInfo {
    pub content_length: usize,
    pub content_hash: odf::Multihash,
    pub content_stream: Option<Box<dyn AsyncRead + Send + Unpin>>,
    pub content_type: Option<MediaType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpdateVersionFileResult {
    pub new_version: FileVersion,
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub content_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateVersionFileUseCaseError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),

    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    TooLarge(#[from] UploadTooLargeError),

    #[error(transparent)]
    RefCASFailed(#[from] RefCASError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Upload of {upload_size} exceeds the {upload_limit} limit")]
pub struct UploadTooLargeError {
    pub upload_size: usize,
    pub upload_limit: usize,
}

impl UploadTooLargeError {
    pub fn new(upload_size: usize, upload_limit: usize) -> Self {
        Self {
            upload_size,
            upload_limit,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetRefUnresolvedError> for UpdateVersionFileUseCaseError {
    fn from(value: odf::DatasetRefUnresolvedError) -> Self {
        match value {
            odf::DatasetRefUnresolvedError::NotFound(err) => Self::NotFound(err),
            odf::DatasetRefUnresolvedError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<PushIngestDataError> for UpdateVersionFileUseCaseError {
    fn from(value: PushIngestDataError) -> Self {
        match value {
            PushIngestDataError::Execution(PushIngestError::CommitError(
                odf::dataset::CommitError::MetadataAppendError(
                    odf::dataset::AppendError::RefCASFailed(e),
                ),
            )) => Self::RefCASFailed(e),
            err => Self::Internal(err.int_err()),
        }
    }
}
