// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use internal_error::InternalError;
use opendatafabric::{MetadataBlock, Multihash};
use thiserror::Error;
use url::Url;

use crate::{
    AccessError,
    BlockNotFoundError,
    ContainsError,
    ExternalTransferOpts,
    GetBlockError,
    GetError,
    GetExternalUrlError,
    GetExternalUrlResult,
    HashMismatchError,
    InsertError,
    InsertOpts,
    InsertResult,
    ObjectNotFoundError,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataBlockRepository: Send + Sync {
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError>;

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetBlockDataError>;

    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError>;

    async fn insert_block_data<'a>(
        &'a self,
        block_data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError>;

    async fn get_block_external_download_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetBlockExternalUrlResult, GetBlockExternalUrlError>;

    async fn get_block_external_upload_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetBlockExternalUrlResult, GetBlockExternalUrlError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Response Results
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InsertBlockResult {
    /// Hash of the inserted object
    pub hash: Multihash,
}

impl From<InsertResult> for InsertBlockResult {
    fn from(InsertResult { hash }: InsertResult) -> Self {
        Self { hash }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct GetBlockExternalUrlResult {
    pub url: Url,
    pub header_map: http::HeaderMap,
    pub expires_at: Option<DateTime<Utc>>,
}

impl From<GetExternalUrlResult> for GetBlockExternalUrlResult {
    fn from(
        GetExternalUrlResult {
            url,
            header_map,
            expires_at,
        }: GetExternalUrlResult,
    ) -> Self {
        Self {
            url,
            header_map,
            expires_at,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Response Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ContainsBlockError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<ContainsError> for ContainsBlockError {
    fn from(v: ContainsError) -> Self {
        match v {
            ContainsError::Access(e) => Self::Access(e),
            ContainsError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetBlockDataError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        BlockNotFoundError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetError> for GetBlockDataError {
    fn from(v: GetError) -> Self {
        match v {
            GetError::NotFound(ObjectNotFoundError { hash }) => {
                Self::NotFound(BlockNotFoundError { hash })
            }
            GetError::Access(e) => Self::Access(e),
            GetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetBlockDataError> for GetBlockError {
    fn from(v: GetBlockDataError) -> Self {
        match v {
            GetBlockDataError::NotFound(e) => Self::NotFound(e),
            GetBlockDataError::Access(e) => Self::Access(e),
            GetBlockDataError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertBlockError {
    #[error(transparent)]
    HashMismatch(
        #[from]
        #[backtrace]
        HashMismatchError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<InsertError> for InsertBlockError {
    fn from(v: InsertError) -> Self {
        match v {
            InsertError::HashMismatch(e) => Self::HashMismatch(e),
            InsertError::Access(e) => Self::Access(e),
            InsertError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetBlockExternalUrlError {
    #[error("Repository does not support external transfers")]
    NotSupported,
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetExternalUrlError> for GetBlockExternalUrlError {
    fn from(v: GetExternalUrlError) -> Self {
        match v {
            GetExternalUrlError::NotSupported => Self::NotSupported,
            GetExternalUrlError::Access(e) => Self::Access(e),
            GetExternalUrlError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
