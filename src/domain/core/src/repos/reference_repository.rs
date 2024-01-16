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
use opendatafabric::Multihash;
use thiserror::Error;

use super::AccessError;
use crate::entities::BlockRef;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait ReferenceRepository {
    /// Resolves reference to the object hash it's pointing to
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError>;

    /// Update reference to point at the specified object hash
    async fn set(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError>;

    /// Deletes specified reference
    async fn delete(&self, r: &BlockRef) -> Result<(), DeleteRefError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetRefError {
    #[error(transparent)]
    NotFound(#[from] RefNotFoundError),
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
pub enum SetRefError {
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
pub enum DeleteRefError {
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
#[error("Reference does not exist: {block_ref:?}")]
pub struct RefNotFoundError {
    pub block_ref: BlockRef,
}
