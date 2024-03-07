// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use internal_error::ResultIntoInternal;
use kamu_core::{Decision, HashedMetadataBlockRef, MetadataBlockTypeFlags, MetadataChainVisitor};
use opendatafabric::{
    AddData,
    AddPushSource,
    DatasetKind,
    MergeStrategy,
    MergeStrategyAppend,
    MetadataEvent,
    Multihash,
    Seed,
    SetDataSchema,
    SetPollingSource,
    SetVocab,
    SourceState,
};

use crate::{DataWriterMetadataState, ScanMetadataError, SourceNotFoundError};

///////////////////////////////////////////////////////////////////////////////

type Flag = MetadataBlockTypeFlags;

///////////////////////////////////////////////////////////////////////////////

pub struct DataWriterDataFusionMetaDataStateVisitor<'a> {
    head: Multihash,
    maybe_source_name: Option<&'a str>,

    next_block_flags: MetadataBlockTypeFlags,
    data_slices: Vec<Multihash>,

    maybe_schema: Option<SchemaRef>,
    maybe_source_event: Option<MetadataEvent>,
    maybe_set_vocab: Option<SetVocab>,

    maybe_prev_checkpoint: Option<Multihash>,
    maybe_prev_watermark: Option<DateTime<Utc>>,
    maybe_prev_source_state: Option<SourceState>,
    maybe_prev_offset: Option<u64>,
}

impl<'a> DataWriterDataFusionMetaDataStateVisitor<'a> {
    pub fn new(head: Multihash, maybe_source_name: Option<&'a str>) -> Self {
        Self {
            head,
            maybe_source_name,

            next_block_flags: Flag::SET_DATA_SCHEMA
                | Flag::ADD_DATA
                | Flag::SET_POLLING_SOURCE
                | Flag::DISABLE_POLLING_SOURCE
                | Flag::ADD_PUSH_SOURCE
                | Flag::DISABLE_PUSH_SOURCE
                | Flag::SET_VOCAB
                | Flag::SEED,
            data_slices: Vec::new(),

            maybe_schema: None,
            maybe_source_event: None,
            maybe_set_vocab: None,

            maybe_prev_checkpoint: None,
            maybe_prev_watermark: None,
            maybe_prev_source_state: None,
            maybe_prev_offset: None,
        }
    }

    pub fn get_metadata_state(self) -> Result<DataWriterMetadataState, ScanMetadataError> {
        let merge_strategy = match (&self.maybe_source_event, self.maybe_source_name) {
            // Source found
            (Some(e), _) => match e {
                MetadataEvent::SetPollingSource(e) => Ok(e.merge.clone()),
                MetadataEvent::AddPushSource(e) => Ok(e.merge.clone()),
                _ => unreachable!(),
            },
            // No source defined - assuming append strategy
            (None, None) => Ok(MergeStrategy::Append(MergeStrategyAppend {})),
            // Source expected but not found
            (None, Some(source)) => Err(SourceNotFoundError::new(
                Some(source),
                format!("Source '{source}' not found"),
            )),
        }?;

        Ok(DataWriterMetadataState {
            head: self.head,
            schema: self.maybe_schema,
            source_event: self.maybe_source_event,
            merge_strategy,
            vocab: self.maybe_set_vocab.unwrap_or_default().into(),
            data_slices: self.data_slices,
            prev_offset: self.maybe_prev_offset,
            prev_checkpoint: self.maybe_prev_checkpoint,
            prev_watermark: self.maybe_prev_watermark,
            prev_source_state: self.maybe_prev_source_state,
        })
    }

    fn handle_set_data_schema(&mut self, e: &SetDataSchema) -> Result<(), ScanMetadataError> {
        self.maybe_schema = Some(e.schema_as_arrow().int_err()?);

        Ok(())
    }

    fn handle_add_data(&mut self, e: &AddData) {
        if let Some(output_data) = &e.new_data {
            self.data_slices.push(output_data.physical_hash.clone());
        }

        if self.maybe_prev_offset.is_none() {
            self.maybe_prev_offset = e.last_offset();
        }

        if self.maybe_prev_checkpoint.is_none() {
            self.maybe_prev_checkpoint =
                e.new_checkpoint.as_ref().map(|cp| cp.physical_hash.clone());
        }

        if self.maybe_prev_watermark.is_none() {
            self.maybe_prev_watermark = e.new_watermark;
        }

        if self.maybe_prev_source_state.is_none() {
            if let Some(ss) = &e.new_source_state
                && let Some(source_name) = self.maybe_source_name
                && source_name != ss.source_name.as_str()
            {
                unimplemented!(
                    "Differentiating between the state of multiple sources is not yet supported"
                );
            }

            self.maybe_prev_source_state = e.new_source_state.clone();
        }
    }

    fn handle_set_polling_source(&mut self, e: &SetPollingSource) -> Result<(), ScanMetadataError> {
        if self.maybe_source_name.is_some() {
            return Err(SourceNotFoundError::new(
                self.maybe_source_name,
                "Expected a named push source, but found polling source",
            )
            .into());
        }

        self.maybe_source_event = Some(e.clone().into());

        Ok(())
    }

    fn handle_add_push_source(&mut self, e: &AddPushSource) -> Result<(), ScanMetadataError> {
        if self.maybe_source_event.is_none() {
            if self.maybe_source_name.is_none()
                || self.maybe_source_name == Some(e.source_name.as_str())
            {
                self.maybe_source_event = Some(e.clone().into());
            }
        } else {
            // Encountered another source - if `source_name` was not specified we
            // return ambiguity error
            if self.maybe_source_name.is_none() {
                return Err(SourceNotFoundError::new(
                    None::<&str>,
                    "Explicit source name is required to pick between several active push sources",
                )
                .into());
            }
        }

        Ok(())
    }

    fn handle_set_vocab(&mut self, e: &SetVocab) {
        self.maybe_set_vocab = Some(e.clone());
    }

    fn handle_seed(&mut self, e: &Seed) {
        assert_eq!(e.dataset_kind, DatasetKind::Root);
    }
}

impl<'a> MetadataChainVisitor for DataWriterDataFusionMetaDataStateVisitor<'a> {
    type VisitError = ScanMetadataError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        match &block.event {
            MetadataEvent::SetDataSchema(e) => {
                self.handle_set_data_schema(e)?;

                self.next_block_flags -= Flag::SET_DATA_SCHEMA;
            }
            MetadataEvent::AddData(e) => {
                self.handle_add_data(e);
            }
            MetadataEvent::SetPollingSource(e) => {
                self.handle_set_polling_source(e)?;

                if self.maybe_source_name.is_none() {
                    self.next_block_flags -= Flag::SET_POLLING_SOURCE;
                }
            }
            MetadataEvent::AddPushSource(e) => {
                self.handle_add_push_source(e)?;

                if self.maybe_source_name.is_some() && self.maybe_source_event.is_some() {
                    self.next_block_flags -= Flag::ADD_PUSH_SOURCE;
                }
            }
            MetadataEvent::SetVocab(e) => {
                self.handle_set_vocab(e);

                self.next_block_flags -= Flag::SET_VOCAB;
            }
            MetadataEvent::Seed(e) => {
                self.handle_seed(e);

                self.next_block_flags -= Flag::SEED;
            }
            MetadataEvent::DisablePollingSource(_) | MetadataEvent::DisablePushSource(_) => {
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::ExecuteTransform(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => {
                unreachable!()
            }
        }

        if !self.next_block_flags.is_empty() {
            Ok(Decision::NextOfType(self.next_block_flags))
        } else {
            Ok(Decision::Stop)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
