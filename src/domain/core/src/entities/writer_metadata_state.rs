// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use opendatafabric as odf;

use super::{
    AcceptVisitorError,
    BlockRef,
    MetadataChainExt,
    ResolvedDataset,
    WriterSourceEventVisitor,
};
use crate::{
    GenericCallbackVisitor,
    MetadataChainVisitorExtInfallible,
    MetadataVisitorDecision,
    PushSourceNotFoundError,
    SearchAddDataVisitor,
    SearchSeedVisitor,
    SearchSetDataSchemaVisitor,
    SearchSetVocabVisitor,
    SearchSourceStateVisitor,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains a projection of the metadata needed for [`DataWriter`] to function
#[derive(Debug, Clone)]
pub struct DataWriterMetadataState {
    pub head: odf::Multihash,
    pub schema: Option<odf::SetDataSchema>,
    pub source_event: Option<odf::MetadataEvent>,
    pub merge_strategy: odf::MergeStrategy,
    pub vocab: odf::DatasetVocabulary,
    pub data_slices: Vec<odf::Multihash>,
    pub prev_offset: Option<u64>,
    pub prev_checkpoint: Option<odf::Multihash>,
    pub prev_watermark: Option<DateTime<Utc>>,
    pub prev_source_state: Option<odf::SourceState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataWriterMetadataState {
    /// Scans metadata chain to populate the needed metadata
    ///
    /// * `source_name` - name of the source to use when extracting the metadata
    ///   needed for writing. Leave empty for polling sources or to use the only
    ///   push source defined when there is no ambiguity.
    pub async fn build(
        target: ResolvedDataset,
        block_ref: &BlockRef,
        source_name: Option<&str>,
    ) -> Result<Self, ScanMetadataError> {
        // TODO: PERF: Full metadata scan below - this is expensive and should be
        //       improved using skip lists.

        let head = target
            .as_metadata_chain()
            .resolve_ref(block_ref)
            .await
            .int_err()?;
        let mut seed_visitor = SearchSeedVisitor::new().adapt_err();
        let mut set_vocab_visitor = SearchSetVocabVisitor::new().adapt_err();
        let mut set_data_schema_visitor = SearchSetDataSchemaVisitor::new().adapt_err();
        let mut prev_source_state_visitor = SearchSourceStateVisitor::new(source_name).adapt_err();
        let mut add_data_visitor = SearchAddDataVisitor::new().adapt_err();
        let mut add_data_collection_visitor = GenericCallbackVisitor::new(
            Vec::new(),
            MetadataVisitorDecision::NextOfType(odf::MetadataEventTypeFlags::ADD_DATA),
            |state, _, block| {
                let odf::MetadataEvent::AddData(e) = &block.event else {
                    unreachable!()
                };

                if let Some(output_data) = &e.new_data {
                    state.push(output_data.physical_hash.clone());
                }

                MetadataVisitorDecision::NextOfType(odf::MetadataEventTypeFlags::ADD_DATA)
            },
        )
        .adapt_err();
        let mut source_event_visitor = WriterSourceEventVisitor::new(source_name);

        target
            .as_metadata_chain()
            .accept_by_hash(
                &mut [
                    &mut source_event_visitor,
                    &mut seed_visitor,
                    &mut set_vocab_visitor,
                    &mut add_data_visitor,
                    &mut set_data_schema_visitor,
                    &mut prev_source_state_visitor,
                    &mut add_data_collection_visitor,
                ],
                &head,
            )
            .await?;

        {
            let seed = seed_visitor
                .into_inner()
                .into_event()
                .expect("Dataset without blocks");

            assert_eq!(seed.dataset_kind, odf::DatasetKind::Root);
        }

        let (source_event, merge_strategy) =
            source_event_visitor.get_source_event_and_merge_strategy()?;
        let (prev_offset, prev_watermark, prev_checkpoint) = {
            match add_data_visitor.into_inner().into_event() {
                Some(e) => (
                    e.last_offset(),
                    e.new_watermark,
                    e.new_checkpoint.map(|cp| cp.physical_hash),
                ),
                None => (None, None, None),
            }
        };
        Ok(Self {
            head,
            schema: set_data_schema_visitor.into_inner().into_event(),
            source_event,
            merge_strategy,
            vocab: set_vocab_visitor
                .into_inner()
                .into_event()
                .unwrap_or_default()
                .into(),
            data_slices: add_data_collection_visitor.into_inner().into_state(),
            prev_offset,
            prev_checkpoint,
            prev_watermark,
            prev_source_state: prev_source_state_visitor.into_inner().into_state(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ScanMetadataError {
    #[error(transparent)]
    SourceNotFound(
        #[from]
        #[backtrace]
        SourceNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<AcceptVisitorError<ScanMetadataError>> for ScanMetadataError {
    fn from(v: AcceptVisitorError<ScanMetadataError>) -> Self {
        match v {
            AcceptVisitorError::Visitor(err) => err,
            AcceptVisitorError::Traversal(err) => Self::Internal(err.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SourceNotFoundError {
    pub source_name: Option<String>,
    message: String,
}

impl SourceNotFoundError {
    pub fn new(source_name: Option<impl Into<String>>, message: impl Into<String>) -> Self {
        Self {
            source_name: source_name.map(std::convert::Into::into),
            message: message.into(),
        }
    }
}

impl From<SourceNotFoundError> for PushSourceNotFoundError {
    fn from(val: SourceNotFoundError) -> Self {
        PushSourceNotFoundError::new(val.source_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
