// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use odf_metadata::MetadataEventTypeFlags;

use crate::{HashedMetadataBlockRef, Infallible};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataVisitorDecision {
    /// Stop Marker. A visitor who has reported the end of work will not be
    /// invited to visit again
    Stop,
    /// A request for the previous block
    Next,
    /// A request for previous blocks, of specific types, using flags.
    ///
    /// # Examples
    /// ```
    /// // Request for SetVocab block
    /// return MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::SET_VOCAB);
    ///
    /// // Request for data blocks (ADD_DATA || EXECUTE_TRANSFORM)
    /// return MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::DATA_BLOCK);
    ///
    /// // Request for a list of blocks of different types
    /// return MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::SET_ATTACHMENTS | MetadataEventTypeFlags::SET_LICENSE);
    /// ```
    NextOfType(MetadataEventTypeFlags),
}

impl MetadataVisitorDecision {
    pub fn merge_decisions(decisions: &[MetadataVisitorDecision]) -> MetadataVisitorDecision {
        let mut merged_flags = MetadataEventTypeFlags::empty();
        for decision in decisions {
            match *decision {
                // Single Next is enough to dominate: if any visitor requested the next block,
                // this cannot be beaten
                MetadataVisitorDecision::Next => return MetadataVisitorDecision::Next,

                // Stop can be ignored
                MetadataVisitorDecision::Stop => { /* continue */ } // Next

                // NextOfType should be unioned
                MetadataVisitorDecision::NextOfType(flags) => {
                    merged_flags |= flags;
                }
            }
        }

        if merged_flags.is_empty() {
            // No flags requested, so we are satisfied
            MetadataVisitorDecision::Stop
        } else {
            // We have a request for a specific set of flags
            MetadataVisitorDecision::NextOfType(merged_flags)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: PERF: MetadataChainVisitor::visit(): use Cow<HashedMetadataBlock>.
//             This will remove some of the cloning.

pub trait MetadataChainVisitor: Send {
    type Error: std::error::Error + Send;

    fn initial_decision(&self) -> MetadataVisitorDecision;

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error>;

    /// Overridden to place logic executed AFTER the end of iterating through
    /// the chain
    fn finish(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Allows syntax such as `let wrapped_visitor = my_visitor.map_err(|e| ...)`.
pub trait MetadataChainVisitorExt<E1>
where
    Self: MetadataChainVisitor<Error = E1>,
    Self: Sized,
    E1: std::error::Error,
{
    fn map_err<E2, F>(self, f: F) -> MetadataChainVisitorMapError<Self, F, E1, E2>
    where
        E2: std::error::Error + Send,
        F: Fn(E1) -> E2 + Send;
}

impl<T, E1> MetadataChainVisitorExt<E1> for T
where
    E1: std::error::Error,
    T: MetadataChainVisitor<Error = E1>,
{
    fn map_err<E2, F>(self, f: F) -> MetadataChainVisitorMapError<Self, F, E1, E2>
    where
        E2: std::error::Error + Send,
        F: Fn(E1) -> E2 + Send,
    {
        MetadataChainVisitorMapError::new(self, f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Allows to convert infallible visitor into a type that agrees with others on
/// what error should be returned. Similar to
/// [`MetadataChainVisitorExt::map_err()`] but for infallible errors.
pub trait MetadataChainVisitorExtInfallible
where
    Self: MetadataChainVisitor<Error = Infallible>,
    Self: Sized,
{
    fn adapt_err<E2>(
        self,
    ) -> MetadataChainVisitorMapError<Self, fn(Infallible) -> E2, Infallible, E2>
    where
        E2: std::error::Error + Send;
}

impl<T> MetadataChainVisitorExtInfallible for T
where
    T: MetadataChainVisitor<Error = Infallible>,
{
    fn adapt_err<E2>(
        self,
    ) -> MetadataChainVisitorMapError<Self, fn(Infallible) -> E2, Infallible, E2>
    where
        E2: std::error::Error + Send,
    {
        MetadataChainVisitorMapError::new(self, Infallible::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wraps the [`MetadataChainVisitor`] to convert the error type, similarly to
/// [`Result::map_err()`]. This is necessary when using several visitors at
/// once, e.g. in the [`MetadataChainExt::accept()`] method to make all visitors
/// agree on one error type..
pub struct MetadataChainVisitorMapError<V, F, E1, E2>
where
    V: MetadataChainVisitor<Error = E1>,
    F: Fn(E1) -> E2,
    E1: std::error::Error,
    E2: std::error::Error,
{
    visitor: V,
    map_err_fn: F,
    _phantom: PhantomData<E2>,
}

impl<V, F, E1, E2> MetadataChainVisitorMapError<V, F, E1, E2>
where
    V: MetadataChainVisitor<Error = E1>,
    F: Fn(E1) -> E2 + Send,
    E1: std::error::Error,
    E2: std::error::Error + Send,
{
    fn new(visitor: V, map_err_fn: F) -> Self {
        Self {
            visitor,
            map_err_fn,
            _phantom: PhantomData,
        }
    }

    pub fn into_inner(self) -> V {
        self.visitor
    }
}

impl<V, F, E1, E2> MetadataChainVisitor for MetadataChainVisitorMapError<V, F, E1, E2>
where
    V: MetadataChainVisitor<Error = E1>,
    F: Fn(E1) -> E2 + Send,
    E1: std::error::Error,
    E2: std::error::Error + Send,
{
    type Error = E2;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        self.visitor.initial_decision()
    }

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        self.visitor
            .visit(hashed_block_ref)
            .map_err(&self.map_err_fn)
    }

    fn finish(&self) -> Result<(), Self::Error> {
        self.visitor.finish().map_err(&self.map_err_fn)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementing this trait for [`Option`] significantly simplifies passing
/// multiple optional visitors into [`MetadataChainExt::accept()`]
impl<T> MetadataChainVisitor for Option<T>
where
    T: MetadataChainVisitor,
{
    type Error = T::Error;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        match self {
            Some(inner) => inner.initial_decision(),
            None => MetadataVisitorDecision::Stop,
        }
    }

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        match self {
            Some(inner) => inner.visit(hashed_block_ref),
            None => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
