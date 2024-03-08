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
    IntoDataStreamBlock,
    MetadataBlock,
    MetadataBlockDataStream,
    MetadataBlockTyped,
    MetadataEvent,
    MetadataEventDataStream,
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

use crate::{
    HashedMetadataBlockRef,
    MetadataBlockTypeFlags as Flag,
    MetadataChainVisitor,
    MetadataVisitorDecision as Decision,
};

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

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let flag = Flag::from(block);

        if !self.requested_flag.contains(flag) {
            return Ok(Decision::NextOfType(self.requested_flag));
        }

        self.hashed_block = Some((hash.clone(), block.clone().into_typed().unwrap()));

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
enum SearchDataBlocksVisitorKind {
    NextDataBlock,
    NextFilledNewWatermark,
    NextFilledNewData,
}

pub struct SearchDataBlocksVisitor<E = InternalError> {
    kind: SearchDataBlocksVisitorKind,
    hashed_data_block: Option<(Multihash, MetadataBlockDataStream)>,
    _phantom: PhantomData<E>,
}

impl<E> SearchDataBlocksVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn next_data_block() -> Self {
        Self::new(SearchDataBlocksVisitorKind::NextDataBlock)
    }

    pub fn next_filled_new_watermark() -> Self {
        Self::new(SearchDataBlocksVisitorKind::NextFilledNewWatermark)
    }

    pub fn next_filled_new_data() -> Self {
        Self::new(SearchDataBlocksVisitorKind::NextFilledNewData)
    }

    fn new(kind: SearchDataBlocksVisitorKind) -> Self {
        Self {
            kind,
            hashed_data_block: None,
            _phantom: PhantomData,
        }
    }

    pub fn into_hashed_data_block(self) -> Option<(Multihash, MetadataBlockDataStream)> {
        self.hashed_data_block
    }

    pub fn into_data_block(self) -> Option<MetadataBlockDataStream> {
        self.hashed_data_block.map(|(_, block)| block)
    }

    pub fn into_event(self) -> Option<MetadataEventDataStream> {
        self.hashed_data_block.map(|(_, block)| block.event)
    }
}

pub struct SearchDataBlocksVisitorFactory<E = InternalError> {
    _phantom: PhantomData<E>,
}

impl<E> MetadataChainVisitor for SearchDataBlocksVisitor<E>
where
    E: Error + Send + Sync,
{
    type Error = E;

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let flag = Flag::from(block);

        if !Flag::DATA_BLOCK.contains(flag) {
            return Ok(Decision::NextOfType(Flag::DATA_BLOCK));
        }

        let data_block = block.as_data_stream_block().unwrap();
        let has_data_block_found = match self.kind {
            SearchDataBlocksVisitorKind::NextDataBlock => true,
            SearchDataBlocksVisitorKind::NextFilledNewWatermark => {
                data_block.event.new_watermark.is_some()
            }
            SearchDataBlocksVisitorKind::NextFilledNewData => data_block.event.new_data.is_some(),
        };

        if has_data_block_found {
            self.hashed_data_block = Some((
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

pub struct SearchNextBlockVisitor<E = InternalError> {
    hashed_block: Option<(Multihash, MetadataBlock)>,
    _phantom: PhantomData<E>,
}

impl<E> Default for SearchNextBlockVisitor<E> {
    fn default() -> Self {
        Self {
            hashed_block: None,
            _phantom: PhantomData,
        }
    }
}

impl<E> SearchNextBlockVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn into_hashed_block(self) -> Option<(Multihash, MetadataBlock)> {
        self.hashed_block
    }

    pub fn into_block(self) -> Option<MetadataBlock> {
        self.hashed_block.map(|(_, block)| block)
    }
}

impl<E> MetadataChainVisitor for SearchNextBlockVisitor<E>
where
    E: Error + Send + Sync,
{
    type Error = E;

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        self.hashed_block = Some((hash.clone(), block.clone()));

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct GenericCallbackVisitor<S, F, E = InternalError> {
    state: S,
    visit_callback: F,
    _phantom: PhantomData<E>,
}

impl<S, F, E> GenericCallbackVisitor<S, F, E>
where
    S: Send + Sync,
    F: Fn(&mut S, HashedMetadataBlockRef) -> Result<Decision, E> + Send + Sync,
    E: Error + Send + Sync,
{
    pub fn new(state: S, visit_callback: F) -> Self {
        Self {
            state,
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
    F: Fn(&mut S, HashedMetadataBlockRef) -> Result<Decision, E> + Send + Sync,
    E: Error + Send + Sync,
{
    type Error = E;

    fn visit(&mut self, hashed_block_ref: HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        (self.visit_callback)(&mut self.state, hashed_block_ref)
    }
}

///////////////////////////////////////////////////////////////////////////////
