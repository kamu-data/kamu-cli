// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::dataset::MetadataVisitorDecision as Decision;
use odf::metadata::MetadataEventTypeFlags as Flag;

use crate::{ScanMetadataError, SourceNotFoundError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WriterSourceEventVisitor<'a> {
    maybe_source_name: Option<&'a str>,
    next_block_flags: Flag,
    maybe_source_event: Option<odf::MetadataEvent>,
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
    ) -> Result<(Option<odf::MetadataEvent>, odf::metadata::MergeStrategy), ScanMetadataError> {
        let merge_strategy = match (&self.maybe_source_event, self.maybe_source_name) {
            // Source found
            (Some(e), _) => match e {
                odf::MetadataEvent::SetPollingSource(e) => Ok(e.merge.clone()),
                odf::MetadataEvent::AddPushSource(e) => Ok(e.merge.clone()),
                _ => unreachable!(),
            },
            // No source defined - assuming append strategy
            (None, None) => Ok(odf::metadata::MergeStrategy::Append(
                odf::metadata::MergeStrategyAppend {},
            )),
            // Source expected but not found
            (None, Some(source)) => Err(SourceNotFoundError::new(
                Some(source),
                format!("Source '{source}' not found"),
            )),
        }?;

        Ok((self.maybe_source_event, merge_strategy))
    }

    fn handle_set_polling_source(
        &mut self,
        e: &odf::metadata::SetPollingSource,
    ) -> Result<(), ScanMetadataError> {
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

    fn handle_add_push_source(
        &mut self,
        e: &odf::metadata::AddPushSource,
    ) -> Result<(), ScanMetadataError> {
        match &self.maybe_source_event {
            None => {
                if self.maybe_source_name.is_none()
                    || self.maybe_source_name == Some(e.source_name.as_str())
                {
                    self.maybe_source_event = Some(e.clone().into());
                }
            }
            Some(odf::MetadataEvent::AddPushSource(s)) if s.source_name == e.source_name => {
                // Encountered previous definition of the same source - the
                // one found first takes precedence
            }
            Some(_) => {
                // Encountered another source - if `source_name` was not specified we
                // return ambiguity error
                if self.maybe_source_name.is_none() {
                    return Err(SourceNotFoundError::new(
                        None::<&str>,
                        "Explicit source name is required to pick between several active push \
                         sources",
                    )
                    .into());
                }
            }
        }

        Ok(())
    }
}

impl odf::dataset::MetadataChainVisitor for WriterSourceEventVisitor<'_> {
    type Error = ScanMetadataError;

    fn initial_decision(&self) -> Decision {
        Decision::NextOfType(self.next_block_flags)
    }

    fn visit(
        &mut self,
        (_, block): odf::dataset::HashedMetadataBlockRef,
    ) -> Result<Decision, Self::Error> {
        match &block.event {
            odf::MetadataEvent::SetPollingSource(e) => {
                self.handle_set_polling_source(e)?;

                if self.maybe_source_name.is_none() {
                    self.next_block_flags -= Flag::SET_POLLING_SOURCE;
                }
            }
            odf::MetadataEvent::AddPushSource(e) => {
                self.handle_add_push_source(e)?;

                if self.maybe_source_name.is_some() && self.maybe_source_event.is_some() {
                    self.next_block_flags -= Flag::ADD_PUSH_SOURCE;
                }
            }
            odf::MetadataEvent::DisablePollingSource(_)
            | odf::MetadataEvent::DisablePushSource(_) => {
                unimplemented!("Disabling sources is not yet fully supported")
            }
            odf::MetadataEvent::Seed(_)
            | odf::MetadataEvent::AddData(_)
            | odf::MetadataEvent::ExecuteTransform(_)
            | odf::MetadataEvent::SetVocab(_)
            | odf::MetadataEvent::SetDataSchema(_)
            | odf::MetadataEvent::SetTransform(_)
            | odf::MetadataEvent::SetAttachments(_)
            | odf::MetadataEvent::SetInfo(_)
            | odf::MetadataEvent::SetLicense(_) => {
                unreachable!()
            }
        }

        Ok(Decision::NextOfType(self.next_block_flags))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
