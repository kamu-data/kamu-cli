// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::Display;

use async_trait::async_trait;
use internal_error::*;
use odf_metadata::*;
use odf_storage::*;
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataChain: Send + Sync {
    /// Detaches this metadata chain from any transaction references
    fn detach_from_transaction(&self) {
        // Nothing to do by default
    }

    /// Returns true if chain contains block
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError>;

    /// Returns size of the specified block in bytes
    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError>;

    /// Returns the specified block as raw bytes
    async fn get_block_bytes(&self, hash: &Multihash) -> Result<bytes::Bytes, GetBlockDataError>;

    /// Returns the specified block
    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    /// Returns the previous block relatively to the specified block,
    /// attempting to use the hint flags as a quick skipping guidance.
    /// In the worst case, returns the nearest previous block.
    /// Tail sequence number represents the minimal allowed block that can be
    /// returned. When omitted, the method will be iterating until the Seed
    async fn get_preceding_block_with_hint(
        &self,
        head_block: &MetadataBlock,
        tail_sequence_number: Option<u64>,
        hint: MetadataVisitorDecision,
    ) -> Result<Option<(Multihash, MetadataBlock)>, GetBlockError>;

    /// Iterates the chain in reverse order starting with specified block and
    /// following the previous block links. The interval returned is `[head,
    /// tail)` - tail is exclusive. If `tail` argument is provided but not
    /// encountered the iteration will continue until first block followed by an
    /// error. If `ignore_missing_tail` argument is provided, the exception
    /// is not generated if tail is not detected while traversing from head
    ///
    /// PERF: iterates over blocks sequentially: O(N). If you initially
    /// know the type of blocks, it's better to consider using accept_*()
    /// API.
    fn iter_blocks_interval<'a>(
        &'a self,
        head_boundary: MetadataChainIterBoundary<'a>,
        tail_boundary: Option<MetadataChainIterBoundary<'a>>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a>;

    /// Appends the block to the chain
    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError>;

    /// Resolves reference to the block hash it's pointing to
    async fn resolve_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError>;

    /// Update reference to point at the specified block
    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetChainRefError>;

    /// Returns storage-level reference repository without any caching involved
    fn as_uncached_ref_repo(&self) -> &dyn ReferenceRepository;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataChainExt: MetadataChain {
    /// Resolves reference to the block hash it's pointing to if it exists
    async fn try_get_ref(&self, r: &BlockRef) -> Result<Option<Multihash>, InternalError> {
        match self.resolve_ref(r).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.int_err()),
        }
    }

    /// Returns the specified block by reference if it exists
    async fn get_block_by_ref(&self, r: &BlockRef) -> Result<MetadataBlock, InternalError> {
        let h = self.resolve_ref(r).await.int_err()?;

        self.get_block(&h).await.int_err()
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

    /// Convenience function to iterate blocks starting with the `head`
    /// reference
    ///
    /// PERF: iterates over blocks sequentially: O(N). If you initially
    /// know the type of blocks, it's better to consider using accept_*()
    /// API.
    fn iter_blocks(&self) -> DynMetadataStream<'_> {
        self.iter_blocks_interval((&BlockRef::Head).into(), None, false)
    }

    /// Convenience function to iterate blocks starting with the specified
    /// reference
    ///
    /// PERF: iterates over blocks sequentially: O(N). If you initially
    /// know the type of blocks, it's better to consider using accept_*()
    /// API.
    fn iter_blocks_ref<'a>(&'a self, head: &'a BlockRef) -> DynMetadataStream<'a> {
        self.iter_blocks_interval(head.into(), None, false)
    }

    /// A method for accepting visitors (see [`MetadataChainVisitor`]) that
    /// allows us to go through the metadata chain once and, if desired,
    /// bypassing blocks of no interest.
    async fn accept<E>(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = E>],
    ) -> Result<(), AcceptVisitorError<E>>
    where
        E: Error + Send,
    {
        self.accept_by_ref(visitors, &BlockRef::Head).await
    }

    /// Same as [`MetadataChainExt::accept()`], allowing us to define the block
    /// (by hash) from which we will start the traverse
    async fn accept_by_hash<E>(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = E>],
        head_hash: &Multihash,
    ) -> Result<(), AcceptVisitorError<E>>
    where
        E: Error + Send,
    {
        self.accept_by_interval(visitors, Some(head_hash), None)
            .await
    }

    /// Same as [`MetadataChainExt::accept()`], allowing us to define the block
    /// (by block reference) from which we will start the traverse
    async fn accept_by_ref<E>(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = E>],
        block_ref: &BlockRef,
    ) -> Result<(), AcceptVisitorError<E>>
    where
        E: Error + Send,
    {
        let block_ref_hash = self
            .resolve_ref(block_ref)
            .await
            .map_err(IterBlocksError::from)?;

        self.accept_by_hash(visitors, &block_ref_hash).await
    }

    /// Same as [`MetadataChainExt::accept()`], allowing us to define the block
    /// interval under which we will be making the traverse.
    ///
    /// Note: the interval is `[head, tail)` - tail is exclusive
    #[tracing::instrument(level = "debug", skip_all, fields(?head_hash, ?tail_hash))]
    async fn accept_by_interval<E>(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = E>],
        head_hash: Option<&Multihash>,
        tail_hash: Option<&Multihash>,
    ) -> Result<(), AcceptVisitorError<E>>
    where
        E: Error + Send,
    {
        self.accept_by_interval_ext(
            visitors,
            head_hash,
            tail_hash,
            AcceptByIntervalOptions {
                inclusive_tail: false,
                ignore_missing_tail: true,
                ignore_hints_when_getting_preceding_block: false,
            },
        )
        .await
    }

    /// Extended version of [`MetadataChainExt::accept_by_interval()`] method,
    /// allowing more flexible configuration.
    #[tracing::instrument(level = "debug", skip_all, fields(?head_hash, ?tail_hash))]
    async fn accept_by_interval_ext<E>(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = E>],
        head_hash: Option<&Multihash>,
        tail_hash: Option<&Multihash>,
        options: AcceptByIntervalOptions,
    ) -> Result<(), AcceptVisitorError<E>>
    where
        E: Error + Send,
    {
        // Collect initial decisions of visitors
        let mut decisions: Vec<_> = visitors
            .iter()
            .map(|visitor| visitor.initial_decision())
            .collect();

        // Merge initial decisions
        let mut merged_decision = MetadataVisitorDecision::merge_decisions(&decisions);

        // Determine starting block
        let mut current_hashed_block = if merged_decision == MetadataVisitorDecision::Stop {
            // All visitors are already satisfied, no need to load any blocks
            None
        } else if let Some(head_hash) = head_hash {
            // Load head block
            let head_block = self
                .get_block(head_hash)
                .await
                .map_err(IterBlocksError::from)?;
            Some((head_hash.clone(), head_block))
        } else {
            // No starting block
            None
        };

        // Determine the sequence number of tail block (for hints).
        let hint_tail_sequence_number = if merged_decision == MetadataVisitorDecision::Stop {
            // No need to iterate
            None
        } else if options.ignore_hints_when_getting_preceding_block {
            // Hints will not be used
            None
        } else if let Some(tail_hash) = tail_hash {
            // Read from the tail block
            Some(
                self.get_block(tail_hash)
                    .await
                    .map_err(IterBlocksError::from)?
                    .sequence_number,
            )
        } else {
            // Tail is the seed block
            None
        };

        // Iterate over blocks until we satisfy all visitors or reach the tail
        while let Some((hash, block)) = &current_hashed_block {
            let is_tail = tail_hash == Some(hash);

            if !options.inclusive_tail && is_tail {
                break;
            }

            // Trace the progress
            tracing::trace!(
                current_block_hash=%hash,
                sequence_number=block.sequence_number,
                hint_tail_sequence_number,
                event_type=?MetadataEventTypeFlags::from(&block.event),
                visitors_decision=?merged_decision,
                "Traversing through block",
            );

            // Feed each visitor with the currently loaded block
            let hashed_block_ref = (hash, block);
            for (decision, visitor) in decisions.iter_mut().zip(visitors.iter_mut()) {
                match decision {
                    MetadataVisitorDecision::Stop => {}
                    MetadataVisitorDecision::NextOfType(type_flags) if type_flags.is_empty() => {
                        *decision = MetadataVisitorDecision::Stop;
                    }
                    MetadataVisitorDecision::Next => {
                        *decision = visitor
                            .visit(hashed_block_ref)
                            .map_err(AcceptVisitorError::Visitor)?;
                    }
                    MetadataVisitorDecision::NextOfType(requested_flags) => {
                        let block_flag = MetadataEventTypeFlags::from(&block.event);

                        if requested_flags.contains(block_flag) {
                            *decision = visitor
                                .visit(hashed_block_ref)
                                .map_err(AcceptVisitorError::Visitor)?;
                        }
                    }
                }
            }

            // Measure the progress after this iteration
            merged_decision = MetadataVisitorDecision::merge_decisions(&decisions);

            // Trace the updated decision
            tracing::trace!(
                updated_visitors_decision=?merged_decision,
                "Block visiting finished",
            );

            // When all visitors are satisfied, no need to load any more blocks
            if merged_decision == MetadataVisitorDecision::Stop {
                break;
            }

            // We have processed the inclusive tail block,
            // not overwriting current_hashed_block for potential checks.
            if is_tail {
                break;
            }

            current_hashed_block = if options.ignore_hints_when_getting_preceding_block {
                // Simply trying to get the previous block
                if let Some(prev_block_hash) = &block.prev_block_hash {
                    let prev_block = self
                        .get_block(prev_block_hash)
                        .await
                        .map_err(IterBlocksError::from)?;
                    Some((prev_block_hash.clone(), prev_block))
                } else {
                    None
                }
            } else {
                // Try to jump to the previous block with satisfaction hints taken into account
                self.get_preceding_block_with_hint(
                    block,
                    hint_tail_sequence_number,
                    merged_decision,
                )
                .await
                .map_err(IterBlocksError::from)?
            }
        }

        // Finish all visitors
        for visitor in visitors {
            visitor.finish().map_err(AcceptVisitorError::Visitor)?;
        }

        // Important: If there was no iteration (head is None), the interval correctness
        //            check will not be performed.
        if !options.ignore_missing_tail
            && let Some((current_hash, _current_block)) = current_hashed_block
            && let Some(tail_hash) = tail_hash
            && current_hash != *tail_hash
            && let Some(head_hash) = head_hash
        {
            Err(IterBlocksError::InvalidInterval(InvalidIntervalError {
                head: head_hash.clone(),
                tail: tail_hash.clone(),
            }))?;
        }

        Ok(())
    }

    /// An auxiliary method that simplifies the work if only one Visitor is
    /// used.
    async fn accept_one<V, E>(&self, mut visitor: V) -> Result<V, AcceptVisitorError<E>>
    where
        V: MetadataChainVisitor<Error = E>,
        E: Error + Send,
    {
        self.accept(&mut [&mut visitor]).await?;

        Ok(visitor)
    }

    /// Same as [`MetadataChainExt::accept_one()`], allowing us to define the
    /// block (by block hash) from which we will start the traverse
    async fn accept_one_by_hash<V, E>(
        &self,
        head_hash: &Multihash,
        mut visitor: V,
    ) -> Result<V, AcceptVisitorError<E>>
    where
        V: MetadataChainVisitor<Error = E>,
        E: Error + Send,
    {
        self.accept_by_interval(&mut [&mut visitor], Some(head_hash), None)
            .await?;

        Ok(visitor)
    }

    /// Method that allows you to apply the reduce operation over a chain
    ///
    /// Note: there is also a method [`MetadataChainExt::try_reduce()`] that
    /// allows you to apply a fallible callback
    async fn reduce<S, F>(
        &self,
        state: S,
        initial_decision: MetadataVisitorDecision,
        callback: F,
    ) -> Result<S, IterBlocksError>
    where
        S: Send,
        F: Fn(&mut S, &Multihash, &MetadataBlock) -> MetadataVisitorDecision + Send,
    {
        let head_hash = self.resolve_ref(&BlockRef::Head).await?;

        Ok(self
            .reduce_by_hash(&head_hash, state, initial_decision, callback)
            .await?)
    }

    /// Same as [`MetadataChainExt::reduce()`], allowing us to define the block
    /// (by hash) from which we will start the traverse
    async fn reduce_by_hash<S, F>(
        &self,
        head_hash: &Multihash,
        state: S,
        initial_decision: MetadataVisitorDecision,
        callback: F,
    ) -> Result<S, IterBlocksError>
    where
        S: Send,
        F: Fn(&mut S, &Multihash, &MetadataBlock) -> MetadataVisitorDecision + Send,
    {
        let mut visitor = GenericCallbackVisitor::new(state, initial_decision, callback);

        self.accept_by_hash(&mut [&mut visitor], head_hash)
            .await
            .map_err(IterBlocksError::from)?;

        Ok(visitor.into_state())
    }

    /// Method that allows you to apply the reduce operation over a chain
    ///
    /// Note: there is also a method [`MetadataChainExt::reduce()`] that allows
    /// you to apply an infallible callback
    async fn try_reduce<S, F, E>(
        &self,
        state: S,
        initial_decision: MetadataVisitorDecision,
        callback: F,
    ) -> Result<S, AcceptVisitorError<E>>
    where
        S: Send,
        E: Error + Send,
        F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<MetadataVisitorDecision, E> + Send,
    {
        let head_hash = self
            .resolve_ref(&BlockRef::Head)
            .await
            .map_err(IterBlocksError::from)?;

        self.try_reduce_by_hash(&head_hash, state, initial_decision, callback)
            .await
    }

    /// Same as [`MetadataChainExt::try_reduce()`], allowing us to define the
    /// block (by hash) from which we will start the traverse
    async fn try_reduce_by_hash<S, F, E>(
        &self,
        head_hash: &Multihash,
        state: S,
        initial_decision: MetadataVisitorDecision,
        callback: F,
    ) -> Result<S, AcceptVisitorError<E>>
    where
        S: Send,
        E: Error + Send,
        F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<MetadataVisitorDecision, E> + Send,
    {
        let mut visitor = GenericFallibleCallbackVisitor::new(state, initial_decision, callback);

        self.accept_by_hash(&mut [&mut visitor], head_hash).await?;

        Ok(visitor.into_state())
    }

    /// Method that searches for the last data block
    async fn last_data_block(&self) -> Result<SearchSingleDataBlockVisitor, IterBlocksError> {
        let visitor = SearchSingleDataBlockVisitor::next();

        self.accept_one(visitor).await.map_err(Into::into)
    }

    /// Same as [`MetadataChainExt::last_data_block()`], but skipping data
    /// blocks that have no real data
    async fn last_data_block_with_new_data(
        &self,
    ) -> Result<SearchSingleDataBlockVisitor, IterBlocksError> {
        let visitor = SearchSingleDataBlockVisitor::next_with_new_data();

        self.accept_one(visitor).await.map_err(Into::into)
    }
}

impl<T> MetadataChainExt for T where T: MetadataChain + ?Sized {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct AcceptByIntervalOptions {
    pub inclusive_tail: bool,
    pub ignore_missing_tail: bool,
    pub ignore_hints_when_getting_preceding_block: bool,
}

impl Default for AcceptByIntervalOptions {
    fn default() -> Self {
        Self {
            inclusive_tail: false,
            ignore_missing_tail: true,
            ignore_hints_when_getting_preceding_block: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct SetRefOpts<'a> {
    /// Ensure new value points to a valid block
    pub validate_block_present: bool,

    /// Validate that old reference still points to the specified block
    /// (compare-and-swap)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    /// Validate that `update_ref` points to the same block as
    /// `block.prev_block_hash` (compare-and-swap)
    pub check_ref_is_prev_block: bool,

    /// Validate that `update_ref` points to the specified block
    /// (compare-and-swap)
    pub check_ref_is: Option<Option<&'a Multihash>>,

    /// Append block using the provided hash computed elsewhere.
    ///
    /// Warning: Use only when you fully trust the source of the precomputed
    /// hash.
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Response Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AcceptVisitorError<E> {
    #[error(transparent)]
    Traversal(#[from] IterBlocksError),

    #[error(transparent)]
    Visitor(E),
}

impl From<AcceptVisitorError<Infallible>> for IterBlocksError {
    fn from(value: AcceptVisitorError<Infallible>) -> Self {
        match value {
            AcceptVisitorError::Traversal(err) => err,
            AcceptVisitorError::Visitor(_) => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetChainRefError {
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

impl From<SetRefError> for SetChainRefError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => Self::Access(e),
            SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct AppendResult {
    pub existing_head: Option<Multihash>,
    pub proposed_head: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

impl From<SetRefError> for AppendError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => Self::Access(e),
            SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<SetChainRefError> for AppendError {
    fn from(v: SetChainRefError) -> Self {
        match v {
            SetChainRefError::BlockNotFound(e) => Self::Internal(e.int_err()),
            SetChainRefError::CASFailed(e) => Self::RefCASFailed(e),
            SetChainRefError::Access(e) => Self::Access(e),
            SetChainRefError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Individual Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: The interval is guaranteed to include the head, but not the tail.
#[derive(Error, Debug)]
#[error("Invalid block interval: {head} (inclusive), {tail}")]
pub struct InvalidIntervalError {
    pub head: Multihash,
    pub tail: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("{reference} expected to point at {expected:?} but points at {actual:?}")]
pub struct RefCASError {
    pub reference: BlockRef,
    pub expected: Option<Multihash>,
    pub actual: Option<Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    #[error(transparent)]
    OffsetsAreNotSequential(#[from] OffsetsNotSequentialError),
    #[error(transparent)]
    InvalidEvent(#[from] InvalidEventError),
    #[error(transparent)]
    NoOpEvent(#[from] NoOpEventError),
}

impl AppendValidationError {
    pub fn no_op_event(event: impl Into<MetadataEvent>, message: impl Into<String>) -> Self {
        Self::NoOpEvent(NoOpEventError::new(event, message))
    }

    pub fn empty_event(event: impl Into<MetadataEvent>) -> Self {
        Self::no_op_event(event, "Event is empty")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("Invalid event: {message}: {event:?}")]
pub struct InvalidEventError {
    event: Box<MetadataEvent>,
    message: String,
}

impl InvalidEventError {
    pub fn new(event: impl Into<MetadataEvent>, message: impl Into<String>) -> Self {
        Self {
            event: Box::new(event.into()),
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
#[error("No-op event: {message}: {event:?}")]
pub struct NoOpEventError {
    event: Box<MetadataEvent>,
    message: String,
}

impl NoOpEventError {
    pub fn new(event: impl Into<MetadataEvent>, message: impl Into<String>) -> Self {
        Self {
            event: Box::new(event.into()),
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
pub struct SequenceIntegrityError {
    pub prev_block_hash: Option<Multihash>,
    pub prev_block_sequence_number: Option<u64>,
    pub next_block_sequence_number: u64,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
pub struct OffsetsNotSequentialError {
    pub expected_offset: u64,
    pub new_offset: u64,
}

impl OffsetsNotSequentialError {
    pub fn new(expected_offset: u64, new_offset: u64) -> Self {
        Self {
            expected_offset,
            new_offset,
        }
    }
}

impl Display for OffsetsNotSequentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected offset interval to start at {} but got {}",
            self.expected_offset, self.new_offset,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
