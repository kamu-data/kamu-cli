// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::{MetadataBlock, Multihash};

use async_trait::async_trait;
use std::pin::Pin;
use thiserror::Error;
use tokio_stream::Stream;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataChain2 {
    /// Resolves reference to the block hash it's pointing to
    async fn get_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError>;

    /// Returns the specified block
    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    /// Iterates the chain in reverse order starting with specified block and following the previous block links.
    /// The interval returned is `[head, tail)` - tail is exclusive.
    /// If `tail` argument is provided but not encountered the iteration will continue until first block followed by an error.
    fn iter_blocks_interval<'a, 'b>(
        &'a self,
        head: &'b Multihash,
        tail: Option<&'b Multihash>,
    ) -> BlockStream<'a>;

    /// Convenience function to iterate blocks starting with the `head` reference
    async fn iter_blocks<'a>(&'a self) -> Result<BlockStream<'a>, GetRefError>;

    /// Convenience function to iterate blocks starting with the specified reference
    async fn iter_blocks_ref<'a>(&'a self, r: &BlockRef) -> Result<BlockStream<'a>, GetRefError>;

    /// Update referece to point at the specified block
    async fn set_ref(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError>;

    /// Appends the block to the chain
    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError>;

    fn as_object_repo(&self) -> &dyn ObjectRepository;
    fn as_reference_repo(&self) -> &dyn ReferenceRepository;
}

/////////////////////////////////////////////////////////////////////////////////////////
// BlockRef
/////////////////////////////////////////////////////////////////////////////////////////

/// References are named pointers to metadata blocks
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BlockRef {
    Head,
}

impl std::fmt::Display for BlockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockRef::Head => write!(f, "head"),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type BlockStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(Multihash, MetadataBlock), IterBlocksError>> + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Expand into bitflags to give fine control
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AppendValidations {
    None,
    Full,
}

#[derive(Clone, Debug)]
pub struct AppendOpts<'a> {
    /// Validations to perform on the newly appended block
    pub validations: AppendValidations,
    /// Update specified reference to the block after appending
    pub update_ref: Option<&'a BlockRef>,
    /// Validate that `update_ref` points to the same block as `block.prev_block_hash` (compare-and-swap)
    pub check_ref_is_prev_block: bool,
    /// Validate that `update_ref` points to the specified block (compare-and-swap)
    pub check_ref_is: Option<Option<&'a Multihash>>,
}

impl<'a> Default for AppendOpts<'a> {
    fn default() -> Self {
        Self {
            validations: AppendValidations::Full,
            update_ref: Some(&BlockRef::Head),
            check_ref_is_prev_block: true,
            check_ref_is: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

pub type InternalError = Box<dyn std::error::Error>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("block does not exist: {hash}")]
pub struct BlockNotFoundError {
    pub hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("block is malformed: {hash}")]
pub struct BlockMalformedError {
    pub hash: Multihash,
    pub source: InternalError,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetBlockError {
    #[error(transparent)]
    NotFound(BlockNotFoundError),
    #[error("internal error")]
    Internal(InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum IterBlocksError {
    #[error(transparent)]
    RefNotFound(RefNotFoundError),
    #[error(transparent)]
    BlockNotFound(BlockNotFoundError),
    #[error(transparent)]
    InvalidInterval(InvalidIntervalError),
    #[error("internal error")]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("interval's tail block was not reached: {tail}")]
pub struct InvalidIntervalError {
    pub tail: Multihash,
}

impl From<GetRefError> for IterBlocksError {
    fn from(e: GetRefError) -> Self {
        match e {
            GetRefError::NotFound(r) => Self::RefNotFound(r),
            GetRefError::Internal(i) => Self::Internal(i),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetRefError {
    #[error(transparent)]
    BlockNotFound(BlockNotFoundError),
    #[error("internal error")]
    Internal(InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("{reference} expected to point at {expected:?} but points at {actual:?}")]
pub struct RefCASError {
    pub reference: BlockRef,
    pub expected: Option<Multihash>,
    pub actual: Option<Multihash>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AppendError {
    #[error(transparent)]
    RefNotFound(RefNotFoundError),
    #[error(transparent)]
    RefCASFailed(RefCASError),
    #[error(transparent)]
    InvalidBlock(#[from] AppendValidationError),
    #[error("internal error")]
    Internal(InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AppendValidationError {
    #[error("first block has to be a seed")]
    FirstBlockMustBeSeed,
    #[error("attempt to append seed block to a non-empty chain")]
    AppendingSeedBlockToNonEmptyChain,
    #[error("invalid previous block")]
    PrevBlockNotFound(BlockNotFoundError),
    #[error("system time has to be monotonically non-decreasing")]
    SystemTimeIsNotMonotonic,
}
