// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use async_utils::AsyncReadObj;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use internal_error::InternalError;
use odf_metadata::*;
use thiserror::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a content-addressable storage
#[async_trait]
pub trait ObjectRepository: Send + Sync {
    /// Returns the protocol implemented by this repository
    fn protocol(&self) -> ObjectRepositoryProtocol;

    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError>;

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError>;

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError>;

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError>;

    /// Returns an object URL for internal operations.
    ///
    /// When, for example, working with S3-backed repo an internal Url will be
    /// in the form of `<s3://bucket/a/b/c>`, so the user has to be
    /// authorized to access the data. To let any authorized user access
    /// the data use [`ObjectRepository::get_external_download_url()`] to issue
    /// a pre-signed URL.
    async fn get_internal_url(&self, hash: &Multihash) -> Url;

    /// Returns a URL that should be accessible to any unauthorized user.
    ///
    /// When, for example, working with S3-backed repo this method will issue a
    /// pre-signed URL that (for a limited time) will be accessible to any user.
    /// For referring to objects internally use
    /// [`ObjectRepository::get_internal_url()`].
    async fn get_external_download_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError>;

    /// Returns a URL which an unauthorized user can upload data to.
    ///
    /// When, for example, working with S3-backed repo this method will issue a
    /// pre-signed URL that (for a limited time) will be accessible to any user
    /// to upload data via HTTP PUT request.
    async fn get_external_upload_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError>;

    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    async fn insert_stream<'a>(
        &'a self,
        src: Box<AsyncReadObj>,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    /// Inserts file by atomic move - only valid for local filesystem repository
    ///
    /// TODO: Consider to move in a separate trait or access via downcasting.
    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes the protocol implemented by an [`ObjectRepository`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRepositoryProtocol {
    Memory,
    LocalFs { base_dir: PathBuf },
    Http,
    S3,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InsertResult {
    /// Hash of the inserted object
    pub hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Default, Debug)]
pub struct InsertOpts<'a> {
    /// Insert object using provided hash computed elsewhere.
    ///
    /// Warning: Use only when you fully trust the source of the precomputed
    /// hash.
    pub precomputed_hash: Option<&'a Multihash>,

    /// Insert will result in error if computed hash does not match this one.
    pub expected_hash: Option<&'a Multihash>,

    /// Hints the size of an object
    pub size_hint: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct ExternalTransferOpts {
    pub expiration: Option<Duration>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct GetExternalUrlResult {
    pub url: Url,
    pub header_map: http::HeaderMap,
    pub expires_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Response Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ContainsError {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        ObjectNotFoundError,
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

impl From<ContainsError> for GetError {
    fn from(v: ContainsError) -> Self {
        match v {
            ContainsError::Access(e) => GetError::Access(e),
            ContainsError::Internal(e) => GetError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetExternalUrlError {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertError {
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

impl From<ContainsError> for InsertError {
    fn from(v: ContainsError) -> Self {
        match v {
            ContainsError::Access(e) => InsertError::Access(e),
            ContainsError::Internal(e) => InsertError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteError {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Individual Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("Object does not exist: {hash}")]
pub struct ObjectNotFoundError {
    pub hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Expected hash {expected} but got {actual}")]
pub struct HashMismatchError {
    pub expected: Multihash,
    pub actual: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
