// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use file_utils::MediaType;
use internal_error::InternalError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Issues URLs that the node serves itself, for storages that cannot give out
/// external download URLs (e.g. the local file system).
///
/// The URL carries a signed, short-lived token and needs no request headers,
/// so callers must check access to the dataset before issuing it.
pub trait FileDownloadService: Send + Sync {
    fn make_download_url(
        &self,
        file: FileDownload,
        opts: odf::storage::ExternalTransferOpts,
    ) -> Result<odf::storage::GetExternalUrlResult, InternalError>;

    fn decode_download_token(&self, token: &str) -> Result<FileDownload, DecodeDownloadTokenError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Object of a dataset that a download token grants access to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDownload {
    pub dataset_id: odf::DatasetID,
    pub content_hash: odf::Multihash,
    pub content_type: MediaType,
    pub content_length: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DecodeDownloadTokenError {
    #[error(transparent)]
    Invalid(#[from] InvalidDownloadTokenError),

    #[error(transparent)]
    Expired(#[from] ExpiredDownloadTokenError),
}

#[derive(Debug, Error)]
#[error("Invalid download token")]
pub struct InvalidDownloadTokenError {}

#[derive(Debug, Error)]
#[error("Download token has expired")]
pub struct ExpiredDownloadTokenError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
