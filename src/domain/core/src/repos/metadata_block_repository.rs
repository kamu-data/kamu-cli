// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::InternalError;
use opendatafabric::{MetadataBlock, Multihash};
use thiserror::Error;

use crate::{
    AccessError,
    ContainsError,
    GetBlockError,
    HashMismatchError,
    InsertError,
    InsertOpts,
    InsertResult,
    ObjectRepository,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataBlockRepository: Send + Sync {
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError>;

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    async fn insert_block<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError>;

    fn as_object_repo(&self) -> &dyn ObjectRepository;
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
