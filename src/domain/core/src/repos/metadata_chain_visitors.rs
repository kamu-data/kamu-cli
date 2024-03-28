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
    AddData,
    AsTypedBlock,
    ExecuteTransform,
    IntoDataStreamBlock,
    MetadataBlock,
    MetadataBlockDataStream,
    MetadataBlockTyped,
    MetadataEvent,
    MetadataEventDataStream,
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

pub struct SearchSetVocabVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetVocabVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetVocab, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_VOCAB)
    }
}

pub struct SearchSeedVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSeedVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<Seed, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SEED)
    }
}

pub struct SearchSetPollingSourceVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetPollingSourceVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetPollingSource, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_POLLING_SOURCE)
    }
}

pub struct SearchSetTransformVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetTransformVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetTransform, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_TRANSFORM)
    }
}

pub struct SearchSetDataSchemaVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetDataSchemaVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetDataSchema, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_DATA_SCHEMA)
    }
}

pub struct SearchExecuteTransformVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchExecuteTransformVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<ExecuteTransform, E> {
        SearchSingleTypedBlockVisitor::new(Flag::EXECUTE_TRANSFORM)
    }
}

pub struct SearchSetInfoVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetInfoVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetInfo, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_INFO)
    }
}

pub struct SearchSetAttachmentsVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetAttachmentsVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetAttachments, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_ATTACHMENTS)
    }
}

pub struct SearchSetLicenseVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchSetLicenseVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<SetLicense, E> {
        SearchSingleTypedBlockVisitor::new(Flag::SET_LICENSE)
    }
}

pub struct SearchAddDataVisitor<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> SearchAddDataVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn create() -> SearchSingleTypedBlockVisitor<AddData, E> {
        SearchSingleTypedBlockVisitor::new(Flag::ADD_DATA)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct SearchSingleTypedBlockVisitor<T, E> {
    requested_flag: Flag,
    hashed_block: Option<(Multihash, MetadataBlockTyped<T>)>,
    _phantom: PhantomData<E>,
}

impl<T, E> SearchSingleTypedBlockVisitor<T, E>
where
    T: VariantOf<MetadataEvent> + Send + Sync,
    E: Error + Send + Sync,
{
    pub fn new(requested_flag: Flag) -> Self {
        Self {
            requested_flag,
            hashed_block: None,
            _phantom: PhantomData,
        }
    }

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

impl<T, E> MetadataChainVisitor for SearchSingleTypedBlockVisitor<T, E>
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

enum SearchSingleDataBlockVisitorKind {
    NextDataBlock,
    NextFilledNewData,
}

pub struct SearchSingleDataBlockVisitor<E = InternalError> {
    kind: SearchSingleDataBlockVisitorKind,
    hashed_block: Option<(Multihash, MetadataBlockDataStream)>,
    _phantom: PhantomData<E>,
}

impl<E> SearchSingleDataBlockVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn next() -> Self {
        Self::new(SearchSingleDataBlockVisitorKind::NextDataBlock)
    }

    pub fn next_with_new_data() -> Self {
        Self::new(SearchSingleDataBlockVisitorKind::NextFilledNewData)
    }

    fn new(kind: SearchSingleDataBlockVisitorKind) -> Self {
        Self {
            kind,
            hashed_block: None,
            _phantom: PhantomData,
        }
    }

    pub fn into_hashed_block(self) -> Option<(Multihash, MetadataBlockDataStream)> {
        self.hashed_block
    }

    pub fn into_block(self) -> Option<MetadataBlockDataStream> {
        self.hashed_block.map(|(_, block)| block)
    }

    pub fn into_event(self) -> Option<MetadataEventDataStream> {
        self.hashed_block.map(|(_, block)| block.event)
    }
}

impl<E> MetadataChainVisitor for SearchSingleDataBlockVisitor<E>
where
    E: Error + Send + Sync,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(Flag::DATA_BLOCK)
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let Some(data_block) = block.as_data_stream_block() else {
            return Ok(Decision::NextOfType(Flag::DATA_BLOCK));
        };

        let has_data_block_found = match self.kind {
            SearchSingleDataBlockVisitorKind::NextDataBlock => true,
            SearchSingleDataBlockVisitorKind::NextFilledNewData => {
                data_block.event.new_data.is_some()
            }
        };

        if has_data_block_found {
            self.hashed_block = Some((
                hash.clone(),
                block.clone().into_data_stream_block().unwrap(),
            ));

            Ok(Decision::Stop)
        } else {
            Ok(Decision::NextOfType(Flag::DATA_BLOCK))
        }
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
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Decision + Send + Sync,
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
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Decision + Send + Sync,
    E: Error + Send + Sync,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        self.initial_decision.clone()
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        Ok((self.visit_callback)(&mut self.state, hash, block))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct GenericFallibleCallbackVisitor<S, F, E = InternalError> {
    state: S,
    initial_decision: Decision,
    fallible_visit_callback: F,
    _phantom: PhantomData<E>,
}

impl<S, F, E> GenericFallibleCallbackVisitor<S, F, E>
where
    S: Send + Sync,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<Decision, E> + Send + Sync,
    E: Error + Send + Sync,
{
    pub fn new(state: S, initial_decision: Decision, visit_callback: F) -> Self {
        Self {
            state,
            initial_decision,
            fallible_visit_callback: visit_callback,
            _phantom: PhantomData,
        }
    }

    pub fn into_state(self) -> S {
        self.state
    }
}

impl<S, F, E> MetadataChainVisitor for GenericFallibleCallbackVisitor<S, F, E>
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
        (self.fallible_visit_callback)(&mut self.state, hash, block)
    }
}

///////////////////////////////////////////////////////////////////////////////
