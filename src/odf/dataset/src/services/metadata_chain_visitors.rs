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
use odf_metadata::{
    AddData,
    AddPushSource,
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
    SourceState,
    VariantOf,
};

use crate::{HashedMetadataBlockRef, MetadataChainVisitor, MetadataVisitorDecision as Decision};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mod infallible {
    /// This is a special error type used for visitors that do not return
    /// errors. Type is declared in a way that it cannot ever be constructed.
    #[derive(thiserror::Error, Debug)]
    #[error("Infallible")]
    pub struct Infallible(u8);

    impl Infallible {
        /// This error can be safely converted into any other error, since it
        /// will never exist in runtime
        pub fn into<T>(self) -> T {
            unreachable!()
        }
    }
}
pub use infallible::Infallible;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! typed_search_single_typed_block_visitor_impl {
    ($name:ident, $event_struct:ty, $block_type_flags:expr) => {
        pub struct $name {}

        impl $name {
            #[allow(clippy::new_ret_no_self)]
            pub fn new() -> SearchSingleTypedBlockVisitor<$event_struct> {
                SearchSingleTypedBlockVisitor::new($block_type_flags)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

typed_search_single_typed_block_visitor_impl!(SearchSetVocabVisitor, SetVocab, Flag::SET_VOCAB);
typed_search_single_typed_block_visitor_impl!(SearchSeedVisitor, Seed, Flag::SEED);
typed_search_single_typed_block_visitor_impl!(
    SearchSetPollingSourceVisitor,
    SetPollingSource,
    Flag::SET_POLLING_SOURCE
);
typed_search_single_typed_block_visitor_impl!(
    SearchSetTransformVisitor,
    SetTransform,
    Flag::SET_TRANSFORM
);
typed_search_single_typed_block_visitor_impl!(
    SearchSetDataSchemaVisitor,
    SetDataSchema,
    Flag::SET_DATA_SCHEMA
);
typed_search_single_typed_block_visitor_impl!(
    SearchExecuteTransformVisitor,
    ExecuteTransform,
    Flag::EXECUTE_TRANSFORM
);
typed_search_single_typed_block_visitor_impl!(SearchSetInfoVisitor, SetInfo, Flag::SET_INFO);
typed_search_single_typed_block_visitor_impl!(
    SearchSetAttachmentsVisitor,
    SetAttachments,
    Flag::SET_ATTACHMENTS
);
typed_search_single_typed_block_visitor_impl!(
    SearchSetLicenseVisitor,
    SetLicense,
    Flag::SET_LICENSE
);
typed_search_single_typed_block_visitor_impl!(SearchAddDataVisitor, AddData, Flag::ADD_DATA);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchSourceStateVisitor<'a> {
    source_name: Option<&'a str>,
    source_state: Option<SourceState>,
}

impl<'a> SearchSourceStateVisitor<'a> {
    pub fn new(source_name: Option<&'a str>) -> Self {
        Self {
            source_name,
            source_state: None,
        }
    }

    pub fn into_state(self) -> Option<SourceState> {
        self.source_state
    }
}

impl MetadataChainVisitor for SearchSourceStateVisitor<'_> {
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(Flag::ADD_DATA)
    }

    fn visit(&mut self, (_hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let MetadataEvent::AddData(e) = &block.event else {
            unreachable!()
        };

        if let Some(ss) = &e.new_source_state
            && let Some(sn) = self.source_name
            && sn != ss.source_name.as_str()
        {
            unimplemented!(
                "Differentiating between the state of multiple sources is not yet supported"
            );
        }

        self.source_state.clone_from(&e.new_source_state);

        Ok(Decision::Stop)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchSingleTypedBlockVisitor<T> {
    requested_flag: Flag,
    hashed_block: Option<(Multihash, MetadataBlockTyped<T>)>,
}

impl<T> SearchSingleTypedBlockVisitor<T>
where
    T: VariantOf<MetadataEvent> + Send,
{
    pub fn new(requested_flag: Flag) -> Self {
        Self {
            requested_flag,
            hashed_block: None,
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

impl<T> MetadataChainVisitor for SearchSingleTypedBlockVisitor<T>
where
    T: VariantOf<MetadataEvent> + Send,
{
    type Error = Infallible;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum SearchSingleDataBlockVisitorKind {
    NextDataBlock,
    NextFilledNewData,
}

pub struct SearchSingleDataBlockVisitor {
    kind: SearchSingleDataBlockVisitorKind,
    hashed_block: Option<(Multihash, MetadataBlockDataStream)>,
}

impl SearchSingleDataBlockVisitor {
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

impl MetadataChainVisitor for SearchSingleDataBlockVisitor {
    type Error = Infallible;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchActivePushSourcesVisitor {
    hashed_blocks: Vec<(Multihash, MetadataBlockTyped<AddPushSource>)>,
}

impl SearchActivePushSourcesVisitor {
    pub fn new() -> Self {
        Self {
            hashed_blocks: vec![],
        }
    }

    pub fn into_hashed_blocks(self) -> Vec<(Multihash, MetadataBlockTyped<AddPushSource>)> {
        self.hashed_blocks
    }
}

impl MetadataChainVisitor for SearchActivePushSourcesVisitor {
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(Flag::ADD_PUSH_SOURCE)
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        // TODO: Add support of `DisablePushSource` events
        let flag = Flag::from(&block.event);

        if flag == Flag::ADD_PUSH_SOURCE {
            self.hashed_blocks
                .push((hash.clone(), block.clone().into_typed().unwrap()));
        }

        Ok(Decision::NextOfType(Flag::ADD_PUSH_SOURCE))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GenericCallbackVisitor<S, F> {
    state: S,
    initial_decision: Decision,
    visit_callback: F,
}

impl<S, F> GenericCallbackVisitor<S, F>
where
    S: Send,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Decision + Send,
{
    pub fn new(state: S, initial_decision: Decision, visit_callback: F) -> Self {
        Self {
            state,
            initial_decision,
            visit_callback,
        }
    }

    pub fn into_state(self) -> S {
        self.state
    }
}

impl<S, F> MetadataChainVisitor for GenericCallbackVisitor<S, F>
where
    S: Send,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Decision + Send,
{
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        self.initial_decision
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        Ok((self.visit_callback)(&mut self.state, hash, block))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GenericFallibleCallbackVisitor<S, F, E = InternalError> {
    state: S,
    initial_decision: Decision,
    fallible_visit_callback: F,
    _phantom: PhantomData<E>,
}

impl<S, F, E> GenericFallibleCallbackVisitor<S, F, E>
where
    S: Send,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<Decision, E> + Send,
    E: Error + Send,
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
    S: Send,
    F: Fn(&mut S, &Multihash, &MetadataBlock) -> Result<Decision, E> + Send,
    E: Error + Send,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        self.initial_decision
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        (self.fallible_visit_callback)(&mut self.state, hash, block)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
