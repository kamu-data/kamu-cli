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

use opendatafabric::{
    AsTypedBlock,
    IntoDataStreamBlock,
    MetadataBlockDataStream,
    MetadataBlockTyped,
    MetadataEvent,
    Multihash,
    Seed,
    SetDataSchema,
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

pub type SearchSetVocabVisitor<E> =
    SearchSingleTypedBlockVisitor<SetVocab, E, { Flag::SET_VOCAB.bits() }>;
pub type SearchSeedVisitor<E> = SearchSingleTypedBlockVisitor<Seed, E, { Flag::SEED.bits() }>;
pub type SearchSetPollingSourceVisitor<E> =
    SearchSingleTypedBlockVisitor<SetPollingSource, E, { Flag::SET_POLLING_SOURCE.bits() }>;
pub type SearchSetTransformVisitor<E> =
    SearchSingleTypedBlockVisitor<SetTransform, E, { Flag::SET_TRANSFORM.bits() }>;
pub type SearchSetDataSchemaVisitor<E> =
    SearchSingleTypedBlockVisitor<SetDataSchema, E, { Flag::SET_DATA_SCHEMA.bits() }>;

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

pub struct SearchDataBlocksVisitor<E> {
    hashed_data_block: Option<(Multihash, MetadataBlockDataStream)>,
    _phantom: PhantomData<E>,
}

impl<E> Default for SearchDataBlocksVisitor<E> {
    fn default() -> Self {
        Self {
            hashed_data_block: None,
            _phantom: PhantomData,
        }
    }
}

impl<E> SearchDataBlocksVisitor<E>
where
    E: Error + Send + Sync,
{
    pub fn into_hashed_data_block(self) -> Option<(Multihash, MetadataBlockDataStream)> {
        self.hashed_data_block
    }

    pub fn into_data_block(self) -> Option<MetadataBlockDataStream> {
        self.hashed_data_block.map(|(_, block)| block)
    }
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

        self.hashed_data_block = Some((
            hash.clone(),
            block.clone().into_data_stream_block().unwrap(),
        ));

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////
