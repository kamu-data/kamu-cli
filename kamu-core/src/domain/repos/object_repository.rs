// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use crate::domain::{BoxedError, InternalError};
use chrono::{DateTime, Utc};
use opendatafabric::Multihash;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use tokio::io::AsyncRead;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents a content-addressable storage
#[async_trait]
pub trait ObjectRepository: Send + Sync {
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError>;

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError>;

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError>;

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError>;

    async fn get_download_url(
        &self,
        prefix_url: &Url,
        hash: &Multihash,
    ) -> Result<(Url, Option<DateTime<Utc>>), GetError>;

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
    /// TODO: Consider to move in a separate trait or access via downcasing.
    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InsertResult {
    pub hash: Multihash,
    pub already_existed: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Default, Debug)]
pub struct InsertOpts<'a> {
    /// Insert object using provided hash computed elsewhere.
    ///
    /// Warning: Use only when you fully trust the source of the precomputed hash.
    pub precomputed_hash: Option<&'a Multihash>,

    /// Insert will result in error if computed hash does not match this one.
    pub expected_hash: Option<&'a Multihash>,

    /// Hints the size of an object
    pub size_hint: Option<usize>,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Response Errors
/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////
// Individual Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AccessError {
    #[error("Respository is read-only")]
    ReadOnly(#[source] Option<BoxedError>),
    #[error("Unauthorized")]
    Unauthorized(#[source] BoxedError),
    #[error("Forbidden")]
    Forbidden(#[source] BoxedError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("Object does not exist: {hash}")]
pub struct ObjectNotFoundError {
    pub hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Expected hash {expected} but got {actual}")]
pub struct HashMismatchError {
    pub expected: Multihash,
    pub actual: Multihash,
}
