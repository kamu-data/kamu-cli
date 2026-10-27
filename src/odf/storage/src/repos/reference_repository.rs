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
use odf_metadata::*;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait ReferenceRepository {
    /// Resolves reference to the object hash it's pointing to
    async fn get(&self, r: &str) -> Result<Multihash, GetRefError>;

    /// Update reference to point at the specified object hash
    async fn set(&self, r: &str, hash: &Multihash) -> Result<(), SetRefError>;

    /// Points the reference at `hash` only if it currently points at
    /// `expected`, or does not exist when `expected` is `None`, as one atomic
    /// step. Storage that cannot do that returns `NotSupported` without
    /// writing anything
    async fn compare_and_set(
        &self,
        r: &str,
        expected: Option<&Multihash>,
        hash: &Multihash,
    ) -> Result<(), CompareAndSetRefError>;

    /// Deletes specified reference
    async fn delete(&self, r: &str) -> Result<(), DeleteRefError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CompareAndSetRefError {
    #[error(transparent)]
    Mismatch(RefMismatchError),
    #[error("Storage does not support conditional writes")]
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
#[error("Reference does not point at the expected block: {block_ref_name}")]
pub struct RefMismatchError {
    pub block_ref_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Reference does not exist: {block_ref_name}")]
pub struct RefNotFoundError {
    pub block_ref_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
