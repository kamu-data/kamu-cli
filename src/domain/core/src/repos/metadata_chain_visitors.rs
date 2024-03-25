// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::marker::PhantomData;

use internal_error::InternalError;
use opendatafabric::{
    AsTypedBlock,
    ExecuteTransform,
    MetadataBlock,
    MetadataBlockTyped,
    MetadataEvent,
    MetadataEventTypeFlags as Flag,
    Multihash,
    Seed,
    SetAttachments,
    SetDataSchema,
    SetInfo,
    SetLicense,
    SetPollingSource,
    SetTransform,
    SetVocab,
    VariantOf,
};

use crate::{HashedMetadataBlockRef, MetadataChainVisitor, MetadataVisitorDecision as Decision};

///////////////////////////////////////////////////////////////////////////////

pub type SearchSetVocabVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetVocab, E, { Flag::SET_VOCAB.bits() }>;

pub type SearchSeedVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<Seed, E, { Flag::SEED.bits() }>;

pub type SearchSetPollingSourceVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetPollingSource, E, { Flag::SET_POLLING_SOURCE.bits() }>;

pub type SearchSetTransformVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetTransform, E, { Flag::SET_TRANSFORM.bits() }>;

pub type SearchSetDataSchemaVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetDataSchema, E, { Flag::SET_DATA_SCHEMA.bits() }>;

pub type SearchExecuteTransformVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<ExecuteTransform, E, { Flag::EXECUTE_TRANSFORM.bits() }>;

pub type SearchSetInfoVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetInfo, E, { Flag::SET_INFO.bits() }>;

pub type SearchSetAttachmentsVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetAttachments, E, { Flag::SET_ATTACHMENTS.bits() }>;

pub type SearchSetLicenseVisitor<E = InternalError> =
    SearchSingleTypedBlockVisitor<SetLicense, E, { Flag::SET_LICENSE.bits() }>;

///////////////////////////////////////////////////////////////////////////////

pub struct SearchSingleTypedBlockVisitor<T, E, const F: u32> {
    requested_flag: Flag,
    hashed_block: Option<(Multihash, MetadataBlockTyped<T>)>,
    _phantom: PhantomData<E>,
}

impl<T, E, const F: u32> Default for SearchSingleTypedBlockVisitor<T, E, F> {
    fn default() -> Self {
        Self {
            requested_flag: Flag::from_bits_retain(F),
            hashed_block: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, E, const F: u32> SearchSingleTypedBlockVisitor<T, E, F>
where
    T: VariantOf<MetadataEvent> + Send + Sync,
    E: Error + Send + Sync,
{
    pub fn into_hashed_block(self) -> Option<(Multihash, MetadataBlockTyped<T>)> {
        self.hashed_block
    }

    pub fn into_block(self) -> Option<MetadataBlockTyped<T>> {
        self.hashed_block.map(|(_, block)| block)
    }

    pub fn into_event(self) -> Option<T> {
        self.hashed_block.map(|(_, block)| block.event)
    }
}

impl<T, E, const F: u32> MetadataChainVisitor for SearchSingleTypedBlockVisitor<T, E, F>
where
    T: VariantOf<MetadataEvent> + Send + Sync,
    E: Error + Send + Sync,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(self.requested_flag)
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let flag = Flag::from(&block.event);

        if !self.requested_flag.contains(flag) {
            unreachable!();
        }

        self.hashed_block = Some((hash.clone(), block.clone().into_typed().unwrap()));

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct GenericCallbackVisitor<S, F, E = InternalError> {
    state: S,
    initial_decision: Decision,
    visit_callback: F,
    _phantom: PhantomData<E>,
}

impl<S, F, E> GenericCallbackVisitor<S, F, E>
where
    S: Send + Sync,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<Decision, E> + Send + Sync,
    E: Error + Send + Sync,
{
    pub fn new(state: S, initial_decision: Decision, visit_callback: F) -> Self {
        Self {
            state,
            initial_decision,
            visit_callback,
            _phantom: PhantomData,
        }
    }

    pub fn into_state(self) -> S {
        self.state
    }
}

impl<S, F, E> MetadataChainVisitor for GenericCallbackVisitor<S, F, E>
where
    S: Send + Sync,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<Decision, E> + Send + Sync,
    E: Error + Send + Sync,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        self.initial_decision.clone()
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        (self.visit_callback)(&mut self.state, hash, block)
    }
}

///////////////////////////////////////////////////////////////////////////////
