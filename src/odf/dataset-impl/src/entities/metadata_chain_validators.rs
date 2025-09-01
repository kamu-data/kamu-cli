// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use odf_dataset::*;
use odf_metadata::*;

use crate::invalid_event;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateSeedBlockOrderVisitor {}

impl ValidateSeedBlockOrderVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        match block.event {
            MetadataEvent::Seed(_) if block.prev_block_hash.is_some() => {
                return Err(AppendValidationError::AppendingSeedBlockToNonEmptyChain);
            }
            MetadataEvent::Seed(_) => (),
            _ if block.prev_block_hash.is_none() => {
                return Err(AppendValidationError::FirstBlockMustBeSeed);
            }
            _ => (),
        }

        Ok(Self {})
    }
}

impl MetadataChainVisitor for ValidateSeedBlockOrderVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Stop
    }

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<MetadataVisitorDecision, Self::Error> {
        unreachable!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateSequenceNumbersIntegrityVisitor {
    appended_sequence_number: u64,
}

impl ValidateSequenceNumbersIntegrityVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        if block.prev_block_hash.is_none() && block.sequence_number != 0 {
            return Err(AppendValidationError::SequenceIntegrity(
                SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: block.sequence_number,
                },
            ));
        }

        Ok(Self {
            appended_sequence_number: block.sequence_number,
        })
    }
}

impl MetadataChainVisitor for ValidateSequenceNumbersIntegrityVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Next
    }

    fn visit(
        &mut self,
        (hash, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        if block.sequence_number != (self.appended_sequence_number - 1) {
            return Err(AppendValidationError::SequenceIntegrity(
                SequenceIntegrityError {
                    prev_block_hash: Some(hash.clone()),
                    prev_block_sequence_number: Some(block.sequence_number),
                    next_block_sequence_number: self.appended_sequence_number,
                },
            ));
        }

        Ok(MetadataVisitorDecision::Stop)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateSystemTimeIsMonotonicVisitor<'a> {
    appended_system_time: &'a DateTime<Utc>,
}

impl<'a> ValidateSystemTimeIsMonotonicVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Self {
        Self {
            appended_system_time: &block.system_time,
        }
    }
}

impl MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor<'_> {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Next
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        if *self.appended_system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic);
        }

        Ok(MetadataVisitorDecision::Stop)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateWatermarkIsMonotonicVisitor {
    is_data_block_appended: bool,
    appended_new_watermark: Option<DateTime<Utc>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    pub fn new(block: &MetadataBlock) -> Self {
        let (is_data_block_appended, appended_new_watermark) =
            if let Some(data_steam_event) = block.event.as_data_stream_event() {
                (true, data_steam_event.new_watermark.copied())
            } else {
                (false, None)
            };

        Self {
            is_data_block_appended,
            appended_new_watermark,
        }
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.is_data_block_appended {
            MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::DATA_BLOCK)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        let Some(data_steam_event) = block.event.as_data_stream_event() else {
            unreachable!()
        };

        match (data_steam_event.new_watermark, &self.appended_new_watermark) {
            (Some(_), None) => Err(AppendValidationError::WatermarkIsNotMonotonic),
            (Some(prev_wm), Some(next_wm)) if prev_wm > next_wm => {
                Err(AppendValidationError::WatermarkIsNotMonotonic)
            }
            _ => Ok(MetadataVisitorDecision::Stop),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateOffsetsAreSequentialVisitor<'a> {
    appended_block_event: &'a MetadataEvent,
    appended_data_block: Option<MetadataBlockDataStreamRef<'a>>,
}

impl<'a> ValidateOffsetsAreSequentialVisitor<'a> {
    fn validate_internal_offset_consistency(
        event: &MetadataEvent,
        data_block: &MetadataBlockDataStreamRef,
    ) -> Result<(), AppendValidationError> {
        if let Some(new_data) = data_block.event.new_data {
            let expected_start_offset = data_block.event.prev_offset.map_or(0, |v| v + 1);

            if new_data.offset_interval.start != expected_start_offset {
                return Err(AppendValidationError::OffsetsAreNotSequential(
                    OffsetsNotSequentialError::new(
                        expected_start_offset,
                        new_data.offset_interval.start,
                    ),
                ));
            }

            if new_data.offset_interval.end < new_data.offset_interval.start {
                invalid_event!(event.clone(), "Invalid offset interval");
            }
        }

        Ok(())
    }

    pub fn new(block: &'a MetadataBlock) -> Result<Self, AppendValidationError> {
        let maybe_data_block = block.as_data_stream_block();

        if let Some(data_block) = &maybe_data_block {
            Self::validate_internal_offset_consistency(&block.event, data_block)?;
        }

        Ok(Self {
            appended_block_event: &block.event,
            appended_data_block: maybe_data_block,
        })
    }
}

impl MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor<'_> {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.appended_data_block.is_some() {
            MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::DATA_BLOCK)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        let Some(data_block) = block.as_data_stream_block() else {
            unreachable!()
        };

        let Some(appended_data_block) = &self.appended_data_block else {
            unreachable!()
        };

        // Validate input/output offset sequencing
        let expected_next_prev_offset = data_block.event.last_offset();

        if appended_data_block.event.prev_offset != expected_next_prev_offset {
            invalid_event!(
                self.appended_block_event.clone(),
                "Carried prev offset does not correspond to the last offset in the chain",
            );
        }

        // Validate internal offset consistency
        if let Some(new_data) = appended_data_block.event.new_data {
            let expected_start_offset = appended_data_block.event.prev_offset.map_or(0, |v| v + 1);

            if new_data.offset_interval.start != expected_start_offset {
                return Err(AppendValidationError::OffsetsAreNotSequential(
                    OffsetsNotSequentialError::new(
                        expected_start_offset,
                        new_data.offset_interval.start,
                    ),
                ));
            }

            if new_data.offset_interval.end < new_data.offset_interval.start {
                invalid_event!(self.appended_block_event.clone(), "Invalid offset interval");
            }
        }

        Ok(MetadataVisitorDecision::Stop)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateUnimplementedEventsVisitor {}

impl ValidateUnimplementedEventsVisitor {
    pub fn new(block: &MetadataBlock) -> Self {
        match &block.event {
            MetadataEvent::DisablePollingSource(_) => {
                // TODO: Ensure has previously active polling source
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::DisablePushSource(_) => {
                // TODO: Ensure has previous push source with matching name
                unimplemented!("Disabling sources is not yet fully supported")
            }
            // TODO: Consider schema evolution rules
            // TODO: Consider what happens with previously defined sources
            MetadataEvent::SetDataSchema(_)
            | MetadataEvent::Seed(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::AddData(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::ExecuteTransform(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::AddPushSource(_)
            | MetadataEvent::SetTransform(_) => {}
        }

        Self {}
    }
}

impl MetadataChainVisitor for ValidateUnimplementedEventsVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Stop
    }

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<MetadataVisitorDecision, Self::Error> {
        unreachable!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateAddPushSourceVisitor {
    is_push_source_appended: bool,
}

impl ValidateAddPushSourceVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        let is_push_source_appended = match &block.event {
            MetadataEvent::AddPushSource(e) => {
                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    validate_transform(&block.event, transform)?;
                }

                true
            }
            _ => false,
        };

        Ok(Self {
            is_push_source_appended,
        })
    }
}

impl MetadataChainVisitor for ValidateAddPushSourceVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.is_push_source_appended {
            MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::SET_POLLING_SOURCE)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        let MetadataEvent::SetPollingSource(e) = &block.event else {
            unreachable!()
        };

        invalid_event!(
            e.clone(),
            "Cannot add a push source while polling source is still active",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateSetPollingSourceVisitor {
    is_set_polling_source_appended: bool,
}

impl ValidateSetPollingSourceVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        let is_set_polling_source_appended = match &block.event {
            MetadataEvent::SetPollingSource(e) => {
                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    validate_transform(&block.event, transform)?;
                }

                // Eth source must identify the chain
                if let FetchStep::EthereumLogs(f) = &e.fetch
                    && f.chain_id.is_none()
                    && f.node_url.is_none()
                {
                    invalid_event!(e.clone(), "Eth source must specify chainId or nodeUrl")
                }

                true
            }
            _ => false,
        };

        Ok(Self {
            is_set_polling_source_appended,
        })
    }
}

impl MetadataChainVisitor for ValidateSetPollingSourceVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.is_set_polling_source_appended {
            MetadataVisitorDecision::NextOfType(MetadataEventTypeFlags::ADD_PUSH_SOURCE)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        let MetadataEvent::AddPushSource(e) = &block.event else {
            unreachable!()
        };

        invalid_event!(
            e.clone(),
            "Cannot add a polling source while some push sources are still active",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateSetTransformVisitor {}

impl ValidateSetTransformVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        if let MetadataEvent::SetTransform(e) = &block.event {
            // Ensure has inputs
            if e.inputs.is_empty() {
                invalid_event!(e.clone(), "Transform must have at least one input");
            }

            let mut duplicate_inputs_id = HashSet::new();
            let mut processed_inputs_id = HashSet::new();

            e.inputs.iter().for_each(|i| {
                if !processed_inputs_id.insert(i.dataset_ref.id()) {
                    duplicate_inputs_id.insert(i.dataset_ref.clone());
                }
            });

            if !duplicate_inputs_id.is_empty() {
                invalid_event!(
                    e.clone(),
                    format!(
                        "Transform contains duplicate inputs: {:?}",
                        duplicate_inputs_id
                    )
                );
            }

            // Ensure inputs are resolved to IDs and aliases are specified
            for i in &e.inputs {
                if i.dataset_ref.id().is_none() || i.alias.is_none() {
                    invalid_event!(
                        e.clone(),
                        "Transform inputs must be resolved to dataset IDs and specify aliases"
                    );
                }
            }

            // Queries must be normalized
            validate_transform(&block.event, &e.transform)?;
        }

        Ok(Self {})
    }
}

impl MetadataChainVisitor for ValidateSetTransformVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Stop
    }

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<MetadataVisitorDecision, Self::Error> {
        unreachable!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateEventIsNotEmptyVisitor {}

impl ValidateEventIsNotEmptyVisitor {
    pub fn new(block: &MetadataBlock) -> Result<Self, AppendValidationError> {
        match &block.event {
            // TODO: ensure only used on Root datasets
            MetadataEvent::AddData(e) if e.is_empty() => {
                return Err(AppendValidationError::empty_event(e.clone()));
            }
            // TODO: ensure only used on Derivative datasets
            MetadataEvent::ExecuteTransform(e) if e.is_empty() => {
                return Err(AppendValidationError::empty_event(e.clone()));
            }
            _ => (),
        }

        Ok(Self {})
    }
}

impl MetadataChainVisitor for ValidateEventIsNotEmptyVisitor {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        MetadataVisitorDecision::Stop
    }

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<MetadataVisitorDecision, Self::Error> {
        unreachable!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateAddDataVisitor<'a> {
    appended_add_data: Option<&'a AddData>,
    prev_schema: Option<SetDataSchema>,
    prev_add_data: Option<AddData>,
    next_block_flags: MetadataEventTypeFlags,
}

impl<'a> ValidateAddDataVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Self {
        let appended_add_data = match &block.event {
            MetadataEvent::AddData(e) => Some(e),
            _ => None,
        };

        Self {
            appended_add_data,
            prev_schema: None,
            prev_add_data: None,
            next_block_flags: MetadataEventTypeFlags::SET_DATA_SCHEMA
                | MetadataEventTypeFlags::ADD_DATA,
        }
    }
}

impl MetadataChainVisitor for ValidateAddDataVisitor<'_> {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.appended_add_data.is_some() {
            MetadataVisitorDecision::NextOfType(self.next_block_flags)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        match &block.event {
            MetadataEvent::AddData(e) => {
                self.prev_add_data = Some(e.clone());
                self.next_block_flags -= MetadataEventTypeFlags::ADD_DATA;
            }
            MetadataEvent::SetDataSchema(e) => {
                self.prev_schema = Some(e.clone());
                self.next_block_flags -= MetadataEventTypeFlags::SET_DATA_SCHEMA;
            }
            _ => unreachable!(),
        }

        Ok(MetadataVisitorDecision::NextOfType(self.next_block_flags))
    }

    fn finish(&self) -> Result<(), Self::Error> {
        let ValidateAddDataVisitor {
            appended_add_data,
            prev_schema,
            prev_add_data,
            ..
        } = self;
        let Some(e) = appended_add_data else {
            return Ok(());
        };

        // Validate schema was defined before adding any data
        if prev_schema.is_none() && e.new_data.is_some() {
            invalid_event!(
                (*e).clone(),
                "SetDataSchema event must be present before adding data",
            );
        }

        let expected_prev_checkpoint = prev_add_data
            .as_ref()
            .and_then(|v| v.new_checkpoint.as_ref())
            .map(|c| &c.physical_hash);
        let prev_watermark = prev_add_data
            .as_ref()
            .and_then(|v| v.new_watermark.as_ref());
        let prev_source_state = prev_add_data
            .as_ref()
            .and_then(|v| v.new_source_state.as_ref());

        // Validate input/output checkpoint sequencing
        if e.prev_checkpoint.as_ref() != expected_prev_checkpoint {
            invalid_event!(
                (*e).clone(),
                "Input checkpoint does not correspond to the last checkpoint in the chain",
            );
        }

        // Validate event advances some state
        if e.new_data.is_none()
            && e.new_checkpoint.as_ref().map(|v| &v.physical_hash) == e.prev_checkpoint.as_ref()
            && e.new_watermark.as_ref() == prev_watermark
            && e.new_source_state.as_ref() == prev_source_state
        {
            return Err(AppendValidationError::no_op_event(
                (*e).clone(),
                "Event neither has data nor it advances checkpoint, watermark, or source state",
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ValidateExecuteTransformVisitor<'a> {
    appended_execute_transform: Option<&'a ExecuteTransform>,
    prev_transform: Option<SetTransform>,
    prev_schema: Option<SetDataSchema>,
    prev_query: Option<ExecuteTransform>,
    next_block_flags: MetadataEventTypeFlags,
}

impl<'a> ValidateExecuteTransformVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Self {
        let appended_execute_transform = match &block.event {
            MetadataEvent::ExecuteTransform(e) => Some(e),
            _ => None,
        };

        Self {
            appended_execute_transform,
            prev_transform: None,
            prev_schema: None,
            prev_query: None,
            next_block_flags: MetadataEventTypeFlags::SET_DATA_SCHEMA
                | MetadataEventTypeFlags::SET_TRANSFORM
                | MetadataEventTypeFlags::EXECUTE_TRANSFORM,
        }
    }
}

impl MetadataChainVisitor for ValidateExecuteTransformVisitor<'_> {
    type Error = AppendValidationError;

    fn initial_decision(&self) -> MetadataVisitorDecision {
        if self.appended_execute_transform.is_some() {
            MetadataVisitorDecision::NextOfType(self.next_block_flags)
        } else {
            MetadataVisitorDecision::Stop
        }
    }

    fn visit(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error> {
        match &block.event {
            MetadataEvent::SetDataSchema(e) => {
                self.prev_schema = Some(e.clone());
                self.next_block_flags -= MetadataEventTypeFlags::SET_DATA_SCHEMA;
            }
            MetadataEvent::SetTransform(e) => {
                self.prev_transform = Some(e.clone());
                self.next_block_flags -= MetadataEventTypeFlags::SET_TRANSFORM;
            }
            MetadataEvent::ExecuteTransform(e) => {
                self.prev_query = Some(e.clone());
                self.next_block_flags -= MetadataEventTypeFlags::EXECUTE_TRANSFORM;
            }
            _ => unreachable!(),
        }

        // Note: `prev_transform` is optional
        if self.prev_schema.is_some() && self.prev_query.is_some() {
            self.next_block_flags -= MetadataEventTypeFlags::SET_TRANSFORM;
        }

        Ok(MetadataVisitorDecision::NextOfType(self.next_block_flags))
    }

    fn finish(&self) -> Result<(), Self::Error> {
        let ValidateExecuteTransformVisitor {
            appended_execute_transform,
            prev_transform,
            prev_schema,
            prev_query,
            ..
        } = self;
        let Some(e) = appended_execute_transform else {
            return Ok(());
        };

        // Validate schema was defined if we're adding data
        if prev_schema.is_none() && e.new_data.is_some() {
            invalid_event!(
                (*e).clone(),
                "SetDataSchema event must be present before adding data",
            );
        }

        // Validate inputs are listed in the same exact order as in SetTransform (or
        // through recursion, in previous ExecuteTransform)
        let actual_inputs = e.query_inputs.iter().map(|i| &i.dataset_id);
        if let Some(prev_transform) = &prev_transform {
            if actual_inputs.ne(prev_transform
                .inputs
                .iter()
                .map(|i| i.dataset_ref.id().unwrap()))
            {
                invalid_event!(
                    (*e).clone(),
                    "Inputs must be listed in same order as initially declared in SetTransform \
                     event",
                );
            }
        } else if let Some(prev_query) = &prev_query {
            if actual_inputs.ne(prev_query.query_inputs.iter().map(|i| &i.dataset_id)) {
                invalid_event!(
                    (*e).clone(),
                    "Inputs must be listed in same order as initially declared in SetTransform \
                     event",
                );
            }
        } else {
            invalid_event!(
                (*e).clone(),
                "ExecuteTransform must be preceded by SetTransform event",
            );
        }

        // Validate input offset and block sequencing
        if let Some(prev_query) = &prev_query {
            for (prev, new) in prev_query.query_inputs.iter().zip(&e.query_inputs) {
                if new.new_block_hash.is_some() && new.new_block_hash == new.prev_block_hash {
                    invalid_event!((*e).clone(), "Invalid input block interval");
                }

                if new.new_offset.is_some() && new.new_offset == new.prev_offset {
                    invalid_event!((*e).clone(), "Invalid input offset interval");
                }

                if new.prev_block_hash.as_ref() != prev.last_block_hash() {
                    invalid_event!(
                        (*e).clone(),
                        "Input prevBlockHash does not correspond to the last block included in \
                         the previous query",
                    );
                }

                if new.prev_offset != prev.last_offset() {
                    invalid_event!(
                        (*e).clone(),
                        "Input prevOffset hash does not correspond to the last offset included in \
                         the previous query",
                    );
                }

                if new.new_offset.is_some() && new.new_block_hash.is_none() {
                    invalid_event!(
                        (*e).clone(),
                        "Input specifies a non-empty offset interval, but its block interval is \
                         empty",
                    );
                }
            }
        }

        let expected_prev_checkpoint = prev_query
            .as_ref()
            .and_then(|v| v.new_checkpoint.as_ref())
            .map(|c| &c.physical_hash);
        let prev_watermark = prev_query.as_ref().and_then(|v| v.new_watermark.as_ref());

        // Validate input/output checkpoint sequencing
        if e.prev_checkpoint.as_ref() != expected_prev_checkpoint {
            invalid_event!(
                (*e).clone(),
                "Input checkpoint does not correspond to the last checkpoint in the chain",
            );
        }

        // Validate event advances some state
        // Note that there can be no data, checkpoint, or watermark in cases when all
        // inputs only had source state updates
        if e.new_data.is_none()
            && e.new_checkpoint.as_ref().map(|v| &v.physical_hash) == e.prev_checkpoint.as_ref()
            && e.new_watermark.as_ref() == prev_watermark
            && e.query_inputs.iter().all(|i| i.new_block_hash.is_none())
        {
            return Err(AppendValidationError::no_op_event(
                (*e).clone(),
                "Event neither has data nor it advances inputs, checkpoint, or watermark",
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_transform(
    e: &MetadataEvent,
    transform: &Transform,
) -> Result<(), AppendValidationError> {
    let Transform::Sql(transform) = transform;
    if transform.query.is_some() {
        invalid_event!(e.clone(), "Transform queries must be normalized");
    }

    if transform.queries.is_none() || transform.queries.as_ref().unwrap().is_empty() {
        invalid_event!(e.clone(), "Transform must have at least one query");
    }

    let queries = transform.queries.as_ref().unwrap();

    if queries.last().unwrap().alias.is_some() {
        invalid_event!(
            e.clone(),
            "Last query in a transform must have no alias an will be treated as an output"
        );
    }

    for q in &queries[..queries.len() - 1] {
        if q.alias.is_none() {
            invalid_event!(
                e.clone(),
                "In a transform all queries except the last one must have aliases"
            );
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
