// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use opendatafabric::{MetadataEventTypeFlags, Multihash};

use crate::HashedMetadataBlockRef;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataVisitorDecision {
    /// Stop Marker. A visitor who has reported the end of work will not be
    /// invited to visit again
    Stop,
    /// A request for the previous block
    Next,
    /// A request for a specific previous block, specifying its hash.
    /// Reserved for long jumps through the metadata chain
    NextWithHash(Multihash),
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

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataChainVisitor: Sync + Send {
    type Error: std::error::Error;

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

///////////////////////////////////////////////////////////////////////////////

/// Trait is needed to generalize [`MetadataChainVisitor`] error type. This is
/// necessary when we are going to use several Visitors at the same time, e.g.
/// using method [`MetadataChainExt::accept()`]
pub trait MetadataChainVisitorHolder: Send {
    type Error: std::error::Error;

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error>;
}

pub struct MetadataChainVisitorHolderImpl<V, F, E1, E2>
where
    V: MetadataChainVisitor<Error = E1>,
    F: Fn(Result<MetadataVisitorDecision, E1>) -> Result<MetadataVisitorDecision, E2>,
    E1: std::error::Error,
    E2: std::error::Error,
{
    visitor: V,
    map_err_fn: F,
    _phantom: PhantomData<E2>,
}

impl<V, F, E1, E2> MetadataChainVisitorHolder for MetadataChainVisitorHolderImpl<V, F, E1, E2>
where
    V: MetadataChainVisitor<Error = E1>,
    F: Fn(Result<MetadataVisitorDecision, E1>) -> Result<MetadataVisitorDecision, E2> + Send,
    E1: std::error::Error,
    E2: std::error::Error + Send,
{
    type Error = E2;

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        match self.visitor.visit(hashed_block_ref) {
            Ok(r) => Ok(r),
            e => (self.map_err_fn)(e),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataChainVisitorExt: MetadataChainVisitor {
    /// Method for converting a [`MetadataChainVisitor`] to
    /// [`MetadataChainVisitorHolder`] using the error conversion function
    fn map_err<'a, E>(
        self,
        map_err_fn: impl Fn(Result<MetadataVisitorDecision, Self::Error>) -> Result<MetadataVisitorDecision, E>
            + Send,
    ) -> impl MetadataChainVisitorHolder<Error = E>
    where
        Self: Sized,
        E: std::error::Error + Send,
    {
        MetadataChainVisitorHolderImpl {
            visitor: self,
            map_err_fn,
            _phantom: PhantomData,
        }
    }
}

impl<T> MetadataChainVisitorExt for T where T: MetadataChainVisitor {}

///////////////////////////////////////////////////////////////////////////////
