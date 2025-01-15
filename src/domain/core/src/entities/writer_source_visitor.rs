// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{
    AddPushSource,
    MergeStrategy,
    MergeStrategyAppend,
    MetadataEvent,
    MetadataEventTypeFlags as Flag,
    SetPollingSource,
};

use crate::{
    HashedMetadataBlockRef,
    MetadataChainVisitor,
    MetadataVisitorDecision as Decision,
    ScanMetadataError,
    SourceNotFoundError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WriterSourceEventVisitor<'a> {
    maybe_source_name: Option<&'a str>,
    next_block_flags: Flag,
    maybe_source_event: Option<MetadataEvent>,
}

impl<'a> WriterSourceEventVisitor<'a> {
    pub fn new(maybe_source_name: Option<&'a str>) -> Self {
        const INITIAL_NEXT_BLOCK_FLAGS: Flag = Flag::SET_POLLING_SOURCE
            .union(Flag::DISABLE_POLLING_SOURCE)
            .union(Flag::ADD_PUSH_SOURCE)
            .union(Flag::DISABLE_PUSH_SOURCE);

        Self {
            maybe_source_name,

            next_block_flags: INITIAL_NEXT_BLOCK_FLAGS,

            maybe_source_event: None,
        }
    }

    pub fn get_source_event_and_merge_strategy(
        self,
    ) -> Result<(Option<MetadataEvent>, MergeStrategy), ScanMetadataError> {
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

        Ok((self.maybe_source_event, merge_strategy))
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
}

impl MetadataChainVisitor for WriterSourceEventVisitor<'_> {
    type Error = ScanMetadataError;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(self.next_block_flags)
    }

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        match &block.event {
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
            MetadataEvent::DisablePollingSource(_) | MetadataEvent::DisablePushSource(_) => {
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::Seed(_)
            | MetadataEvent::AddData(_)
            | MetadataEvent::ExecuteTransform(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetDataSchema(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => {
                unreachable!()
            }
        }

        Ok(Decision::NextOfType(self.next_block_flags))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
