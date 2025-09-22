// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::marker::PhantomData;

use internal_error::InternalError;
use odf_metadata::{
    AddData,
    AddPushSource,
    AsTypedBlock,
    DatasetKind,
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

use crate::{
    HashedMetadataBlock,
    HashedMetadataBlockRef,
    MetadataChainVisitor,
    MetadataVisitorDecision as Decision,
};

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
            #[expect(clippy::new_ret_no_self)]
            pub fn new() -> SearchSingleTypedBlockVisitor<$event_struct> {
                SearchSingleTypedBlockVisitor::new($block_type_flags)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! typed_kind_based_search_single_typed_block_visitor_impl {
    ($expected_kind:expr, $name:ident, $event_struct:ty, $block_type_flags:expr) => {
        pub struct $name {}

        impl $name {
            #[expect(clippy::new_ret_no_self)]
            pub fn new(
                actual_dataset_kind: DatasetKind,
            ) -> DatasetKindBasedVisitor<SearchSingleTypedBlockVisitor<$event_struct>, Infallible>
            {
                let visitor = SearchSingleTypedBlockVisitor::new($block_type_flags);
                DatasetKindBasedVisitor::new($expected_kind, actual_dataset_kind, visitor)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

typed_search_single_typed_block_visitor_impl!(SearchSetVocabVisitor, SetVocab, Flag::SET_VOCAB);
typed_search_single_typed_block_visitor_impl!(SearchSeedVisitor, Seed, Flag::SEED);
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
typed_kind_based_search_single_typed_block_visitor_impl!(
    DatasetKind::Root,
    SearchAddDataVisitor,
    AddData,
    Flag::ADD_DATA
);

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

pub struct DatasetKindBasedVisitor<V, E>
where
    V: MetadataChainVisitor<Error = E>,
{
    expected_dataset_kind: DatasetKind,
    actual_dataset_kind: DatasetKind,
    visitor: V,
}

impl<V, E> DatasetKindBasedVisitor<V, E>
where
    V: MetadataChainVisitor<Error = E>,
{
    pub fn new(
        expected_dataset_kind: DatasetKind,
        actual_dataset_kind: DatasetKind,
        visitor: V,
    ) -> Self {
        Self {
            expected_dataset_kind,
            actual_dataset_kind,
            visitor,
        }
    }

    pub fn into_inner(self) -> V {
        self.visitor
    }
}

impl<V, E> MetadataChainVisitor for DatasetKindBasedVisitor<V, E>
where
    V: MetadataChainVisitor<Error = E>,
    E: Error + Send,
{
    type Error = E;

    fn initial_decision(&self) -> Decision {
        if self.expected_dataset_kind != self.actual_dataset_kind {
            Decision::Stop
        } else {
            self.visitor.initial_decision()
        }
    }

    #[inline]
    fn visit(&mut self, hashed_block_ref: HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        self.visitor.visit(hashed_block_ref)
    }

    fn finish(&self) -> Result<(), Self::Error> {
        self.visitor.finish()
    }
}

impl<V, E> std::ops::Deref for DatasetKindBasedVisitor<V, E>
where
    V: MetadataChainVisitor<Error = E>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.visitor
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
    dataset_kind: DatasetKind,
    disabled_push_source_names: HashSet<String>,
    active_push_sources: VecDeque<(Multihash, MetadataBlockTyped<AddPushSource>)>,
}

impl SearchActivePushSourcesVisitor {
    const ROOT_SOURCE_RELATED_FLAGS: Flag = Flag::from_bits_retain(
        Flag::SET_POLLING_SOURCE.bits()
            | Flag::DISABLE_POLLING_SOURCE.bits()
            | Flag::ADD_PUSH_SOURCE.bits()
            | Flag::DISABLE_PUSH_SOURCE.bits(),
    );

    pub fn new(dataset_kind: DatasetKind) -> Self {
        Self {
            dataset_kind,
            disabled_push_source_names: HashSet::new(),
            active_push_sources: VecDeque::new(),
        }
    }

    pub fn into_hashed_blocks(self) -> Vec<(Multihash, MetadataBlockTyped<AddPushSource>)> {
        // No reallocation
        Vec::from(self.active_push_sources)
    }
}

impl MetadataChainVisitor for SearchActivePushSourcesVisitor {
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        match self.dataset_kind {
            DatasetKind::Root => Decision::NextOfType(Self::ROOT_SOURCE_RELATED_FLAGS),
            DatasetKind::Derivative => Decision::Stop,
        }
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        match &block.event {
            MetadataEvent::SetPollingSource(_) | MetadataEvent::DisablePollingSource(_) => {
                // > Push and polling sources are mutually exclusive.
                // > (c) RFC-011 (https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/011-push-ingest-sources.md#guide-level-explanation)
                //
                // Thus, if we encounter anything related to a polling source,
                // it means earlier push source events are no longer relevant.
                return Ok(Decision::Stop);
            }
            MetadataEvent::AddPushSource(add_push_source) => {
                // Since we move backwards, we already know in advance whether a source is
                // disabled.
                let is_not_disabled = !self
                    .disabled_push_source_names
                    .contains(&add_push_source.source_name);

                if is_not_disabled {
                    // SAFETY: block type verified
                    let typed_block = block.clone().into_typed().unwrap();
                    self.active_push_sources
                        .push_front((hash.clone(), typed_block));
                }
            }
            MetadataEvent::DisablePushSource(disable_push_source) => {
                // Within this visitor, we assume the correctness of the metadata chain was
                // already validated during event appending.
                self.disabled_push_source_names
                    .insert(disable_push_source.source_name.clone());
            }
            _ => {
                unreachable!()
            }
        }

        Ok(Decision::NextOfType(Self::ROOT_SOURCE_RELATED_FLAGS))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchActivePollingSourceVisitor {
    dataset_kind: DatasetKind,
    hashed_block: Option<(Multihash, MetadataBlockTyped<SetPollingSource>)>,
}

impl SearchActivePollingSourceVisitor {
    const ROOT_SOURCE_RELATED_FLAGS: Flag = Flag::from_bits_retain(
        Flag::SET_POLLING_SOURCE.bits()
            | Flag::DISABLE_POLLING_SOURCE.bits()
            | Flag::ADD_PUSH_SOURCE.bits()
            | Flag::DISABLE_PUSH_SOURCE.bits(),
    );

    pub fn new(dataset_kind: DatasetKind) -> Self {
        Self {
            dataset_kind,
            hashed_block: None,
        }
    }

    pub fn into_hashed_block(self) -> Option<(Multihash, MetadataBlockTyped<SetPollingSource>)> {
        self.hashed_block
    }
}

impl MetadataChainVisitor for SearchActivePollingSourceVisitor {
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        match self.dataset_kind {
            DatasetKind::Root => Decision::NextOfType(Self::ROOT_SOURCE_RELATED_FLAGS),
            DatasetKind::Derivative => Decision::Stop,
        }
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        #[expect(clippy::match_same_arms)]
        match &block.event {
            MetadataEvent::AddPushSource(_) | MetadataEvent::DisablePushSource(_) => {
                // > Push and polling sources are mutually exclusive.
                // > (c) RFC-011 (https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/011-push-ingest-sources.md#guide-level-explanation)
                //
                // Thus, if we encounter anything related to push sources,
                // it means earlier pulling source events are no longer
                // relevant.
            }
            MetadataEvent::SetPollingSource(_) => {
                // SAFETY: block type verified
                let typed_block = block.clone().into_typed().unwrap();
                self.hashed_block = Some((hash.clone(), typed_block));
            }
            MetadataEvent::DisablePollingSource(_) => {
                // No active one exists
            }
            _ => unreachable!(),
        }

        Ok(Decision::Stop)
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

pub struct SearchKeyBlocksVisitor {
    key_blocks: Vec<HashedMetadataBlock>,
    key_event_flags: Flag,
}

impl SearchKeyBlocksVisitor {
    pub fn new() -> Self {
        Self {
            key_blocks: vec![],
            key_event_flags: Flag::empty(),
        }
    }

    pub fn key_event_flags(&self) -> Flag {
        self.key_event_flags
    }

    pub fn into_hashed_key_blocks(self) -> Vec<HashedMetadataBlock> {
        self.key_blocks
    }
}

impl MetadataChainVisitor for SearchKeyBlocksVisitor {
    type Error = Infallible;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(Flag::KEY_BLOCK)
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let flag = Flag::from(&block.event);

        if Flag::KEY_BLOCK.contains(flag) {
            self.key_event_flags |= flag;
            self.key_blocks.push((hash.clone(), block.clone()));
        }

        Ok(Decision::NextOfType(Flag::KEY_BLOCK))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
