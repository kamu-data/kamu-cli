// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use super::metadata_stream::DynMetadataStream;
use crate::domain::*;
use opendatafabric::{MetadataBlock, MetadataEvent, Multihash};

use async_trait::async_trait;
use strum_macros::EnumString;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataChain: Send + Sync {
    /// Resolves reference to the block hash it's pointing to
    async fn get_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError>;

    /// Returns the specified block
    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    /// Iterates the chain in reverse order starting with specified block and following the previous block links.
    /// The interval returned is `[head, tail)` - tail is exclusive.
    /// If `tail` argument is provided but not encountered the iteration will continue until first block followed by an error.
    /// If `ignore_missing_tail` argument is provided, the exception is not generated if tail is not detected while traversing from head
    fn iter_blocks_interval<'a>(
        &'a self,
        head: &'a Multihash,
        tail: Option<&'a Multihash>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a>;

    // TODO: Remove this method by allowing BlockRefs to be either tags or hashes
    fn iter_blocks_interval_ref<'a>(
        &'a self,
        head: &'a BlockRef,
        tail: Option<&'a BlockRef>,
    ) -> DynMetadataStream<'a>;

    /// Update referece to point at the specified block
    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetRefError>;

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
// Helpers
/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataChainExt: MetadataChain {
    /// Resolves reference to the block hash it's pointing to if it exists
    async fn try_get_ref(&self, r: &BlockRef) -> Result<Option<Multihash>, InternalError> {
        match self.get_ref(r).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.int_err()),
        }
    }

    /// Returns the specified block if it exists
    async fn try_get_block(
        &self,
        hash: &Multihash,
    ) -> Result<Option<MetadataBlock>, InternalError> {
        match self.get_block(hash).await {
            Ok(b) => Ok(Some(b)),
            Err(GetBlockError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.int_err()),
        }
    }

    /// Convenience function to iterate blocks starting with the `head` reference
    fn iter_blocks<'a>(&'a self) -> DynMetadataStream<'a> {
        self.iter_blocks_interval_ref(&BlockRef::Head, None)
    }

    /// Convenience function to iterate blocks starting with the specified reference
    fn iter_blocks_ref<'a>(&'a self, head: &'a BlockRef) -> DynMetadataStream<'a> {
        self.iter_blocks_interval_ref(&head, None)
    }
}

impl<T> MetadataChainExt for T where T: MetadataChain + ?Sized {}

/////////////////////////////////////////////////////////////////////////////////////////
// BlockRef
/////////////////////////////////////////////////////////////////////////////////////////

/// References are named pointers to metadata blocks
#[derive(EnumString, Clone, PartialEq, Eq, Debug)]
pub enum BlockRef {
    #[strum(serialize = "head")]
    Head,
}

impl BlockRef {
    pub fn as_str(&self) -> &str {
        match self {
            BlockRef::Head => "head",
        }
    }
}

impl AsRef<str> for BlockRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for BlockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct SetRefOpts<'a> {
    /// Ensure new value points to a valid block
    pub validate_block_present: bool,

    /// Validate that old reference still points to the specified block (compare-and-swap)
    pub check_ref_is: Option<Option<&'a Multihash>>,
}

impl Default for SetRefOpts<'_> {
    fn default() -> Self {
        Self {
            validate_block_present: true,
            check_ref_is: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Expand into bitflags to give fine control
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AppendValidation {
    None,
    Full,
}

#[derive(Clone, Debug)]
pub struct AppendOpts<'a> {
    /// Validations to perform on the newly appended block
    pub validation: AppendValidation,

    /// Update specified reference to the block after appending
    pub update_ref: Option<&'a BlockRef>,

    /// Validate that `update_ref` points to the same block as `block.prev_block_hash` (compare-and-swap)
    pub check_ref_is_prev_block: bool,

    /// Validate that `update_ref` points to the specified block (compare-and-swap)
    pub check_ref_is: Option<Option<&'a Multihash>>,

    /// Append block using the provided hash computed elsewhere.
    ///
    /// Warning: Use only when you fully trust the source of the precomputed hash.
    pub precomputed_hash: Option<&'a Multihash>,

    /// Append will result in error if computed hash does not match this one.
    pub expected_hash: Option<&'a Multihash>,
}

impl Default for AppendOpts<'_> {
    fn default() -> Self {
        Self {
            validation: AppendValidation::Full,
            update_ref: Some(&BlockRef::Head),
            check_ref_is_prev_block: true,
            check_ref_is: None,
            precomputed_hash: None,
            expected_hash: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Response Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetBlockError {
    #[error(transparent)]
    NotFound(#[from] BlockNotFoundError),
    #[error(transparent)]
    BlockVersion(#[from] BlockVersionError),
    #[error(transparent)]
    BlockMalformed(#[from] BlockMalformedError),
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
pub enum IterBlocksError {
    #[error(transparent)]
    RefNotFound(RefNotFoundError),
    #[error(transparent)]
    BlockNotFound(BlockNotFoundError),
    #[error(transparent)]
    BlockVersion(BlockVersionError),
    #[error(transparent)]
    BlockMalformed(#[from] BlockMalformedError),
    #[error(transparent)]
    InvalidInterval(InvalidIntervalError),
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

impl From<GetRefError> for IterBlocksError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => Self::RefNotFound(e),
            GetRefError::Access(e) => Self::Access(e),
            GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetBlockError> for IterBlocksError {
    fn from(v: GetBlockError) -> Self {
        match v {
            GetBlockError::NotFound(e) => Self::BlockNotFound(e),
            GetBlockError::BlockVersion(e) => Self::BlockVersion(e),
            GetBlockError::BlockMalformed(e) => Self::BlockMalformed(e),
            GetBlockError::Access(e) => Self::Access(e),
            GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetRefError {
    #[error(transparent)]
    BlockNotFound(BlockNotFoundError),
    #[error(transparent)]
    CASFailed(#[from] RefCASError),
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

impl From<super::reference_repository::SetRefError> for SetRefError {
    fn from(v: super::reference_repository::SetRefError) -> Self {
        match v {
            super::reference_repository::SetRefError::Access(e) => Self::Access(e),
            super::reference_repository::SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AppendError {
    #[error(transparent)]
    RefNotFound(#[from] RefNotFoundError),
    #[error(transparent)]
    RefCASFailed(#[from] RefCASError),
    #[error(transparent)]
    InvalidBlock(#[from] AppendValidationError),
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

impl From<super::reference_repository::SetRefError> for AppendError {
    fn from(v: super::reference_repository::SetRefError) -> Self {
        match v {
            super::reference_repository::SetRefError::Access(e) => Self::Access(e),
            super::reference_repository::SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<SetRefError> for AppendError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::BlockNotFound(e) => Self::Internal(e.int_err()),
            SetRefError::CASFailed(e) => Self::RefCASFailed(e),
            SetRefError::Access(e) => Self::Access(e),
            SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Individual Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Block has incompatible version: {hash}")]
pub struct BlockVersionError {
    pub hash: Multihash,
    pub source: BoxedError,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Block does not exist: {hash}")]
pub struct BlockNotFoundError {
    pub hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Block is malformed: {hash}")]
pub struct BlockMalformedError {
    pub hash: Multihash,
    pub source: BoxedError,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Invalid block interval [{head}, {tail})")]
pub struct InvalidIntervalError {
    pub head: Multihash,
    pub tail: Multihash,
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
pub enum AppendValidationError {
    #[error(transparent)]
    HashMismatch(#[from] HashMismatchError),
    #[error("First block has to be a seed, perhaps new block does not link to the previous")]
    FirstBlockMustBeSeed,
    #[error("Attempt to append seed block to a non-empty chain")]
    AppendingSeedBlockToNonEmptyChain,
    #[error("Invalid previous block")]
    PrevBlockNotFound(#[from] BlockNotFoundError),
    #[error(transparent)]
    SequenceIntegrity(#[from] SequenceIntegrityError),
    #[error("System time has to be monotonically non-decreasing")]
    SystemTimeIsNotMonotonic,
    #[error("Watermark has to be monotonically increasing")]
    WatermarkIsNotMonotonic,
    #[error("Data offsets have to be sequential")]
    OffsetsAreNotSequential,
    #[error(transparent)]
    InvalidEvent(#[from] InvalidEventError),
    #[error(transparent)]
    NoOpEvent(#[from] NoOpEventError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("Invalid event: {message}: {event:?}")]
pub struct InvalidEventError {
    event: MetadataEvent,
    message: String,
}

impl InvalidEventError {
    pub fn new(event: impl Into<MetadataEvent>, message: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            message: message.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("No-op event: {message}: {event:?}")]
pub struct NoOpEventError {
    event: MetadataEvent,
    message: String,
}

impl NoOpEventError {
    pub fn new(event: impl Into<MetadataEvent>, message: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            message: message.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
pub struct SequenceIntegrityError {
    pub prev_block_hash: Option<Multihash>,
    pub prev_block_sequence_number: Option<i32>,
    pub next_block_sequence_number: i32,
}

impl Display for SequenceIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(prev_block_hash) = &self.prev_block_hash {
            write!(
                f,
                "Block {} with sequence number {} cannot be followed by block with sequence \
                 number {}",
                prev_block_hash,
                self.prev_block_sequence_number.unwrap(),
                self.next_block_sequence_number
            )
        } else {
            write!(
                f,
                "Block sequence has to start with zero, not {}",
                self.next_block_sequence_number
            )
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
