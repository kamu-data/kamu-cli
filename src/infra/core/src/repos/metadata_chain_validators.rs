// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::{
    AppendError,
    AppendValidationError,
    BlockNotFoundError,
    HashedMetadataBlockRef,
    MetadataChainVisitor,
    MetadataVisitorDecision as Decision,
    OffsetsNotSequentialError,
    SequenceIntegrityError,
};
use opendatafabric::{
    AddData,
    AddPushSource,
    ExecuteTransform,
    IntoDataStreamBlock,
    IntoDataStreamEvent,
    MetadataBlock,
    MetadataBlockDataStreamRef,
    MetadataEvent,
    MetadataEventTypeFlags as Flag,
    Multihash,
    SetDataSchema,
    SetPollingSource,
    SetTransform,
    Transform,
};

use crate::invalid_event;

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSeedBlockOrderVisitor<'a> {
    is_seed_appended_block: bool,
    appended_prev_block_hash: Option<&'a Multihash>,
}

impl<'a> ValidateSeedBlockOrderVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Self {
        Self {
            is_seed_appended_block: matches!(&block.event, MetadataEvent::Seed(_)),
            appended_prev_block_hash: block.prev_block_hash.as_ref(),
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateSeedBlockOrderVisitor<'a> {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        match (
            self.is_seed_appended_block,
            self.appended_prev_block_hash.is_some(),
        ) {
            (true, true) => Err(AppendValidationError::AppendingSeedBlockToNonEmptyChain.into()),
            (false, false) => Err(AppendValidationError::FirstBlockMustBeSeed.into()),
            (_, _) => Ok(Decision::Stop),
        }
    }

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        unreachable!()
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidatePrevBlockExistsVisitor<'a> {
    appended_prev_block_hash: Option<&'a Multihash>,
}

impl<'a> ValidatePrevBlockExistsVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Self {
        Self {
            appended_prev_block_hash: block.prev_block_hash.as_ref(),
        }
    }
}

impl<'a> MetadataChainVisitor for ValidatePrevBlockExistsVisitor<'a> {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        if self.appended_prev_block_hash.is_some() {
            Ok(Decision::Next)
        } else {
            Ok(Decision::Stop)
        }
    }

    fn visit(&mut self, (hash, _): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let Some(appended_prev_block_hash) = self.appended_prev_block_hash else {
            unreachable!()
        };

        if appended_prev_block_hash != hash {
            return Err(
                AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                    hash: appended_prev_block_hash.clone(),
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSequenceNumbersIntegrityVisitor {
    appended_sequence_number: u64,
    has_appended_prev_block_hash: bool,
}

impl ValidateSequenceNumbersIntegrityVisitor {
    pub fn new(block: &MetadataBlock) -> Self {
        Self {
            has_appended_prev_block_hash: block.prev_block_hash.is_some(),
            appended_sequence_number: block.sequence_number,
        }
    }
}

impl MetadataChainVisitor for ValidateSequenceNumbersIntegrityVisitor {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        if !self.has_appended_prev_block_hash && self.appended_sequence_number != 0 {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: self.appended_sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Next)
    }

    fn visit(&mut self, (hash, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        if block.sequence_number != (self.appended_sequence_number - 1) {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: Some(hash.clone()),
                    prev_block_sequence_number: Some(block.sequence_number),
                    next_block_sequence_number: self.appended_sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

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

impl<'a> MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor<'a> {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        Ok(Decision::Next)
    }

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        if *self.appended_system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateWatermarkIsMonotonicVisitor {
    #[allow(clippy::option_option)]
    appended_new_watermark: Option<Option<DateTime<Utc>>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    pub fn new(block: &MetadataBlock) -> Self {
        let appended_new_watermark = block
            .event
            .as_data_stream_event()
            .map(|data_block| data_block.new_watermark.copied());

        Self {
            appended_new_watermark,
        }
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        if self.appended_new_watermark.is_some() {
            Ok(Decision::NextOfType(Flag::DATA_BLOCK))
        } else {
            Ok(Decision::Stop)
        }
    }

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        let Some(data_steam_event) = block.event.as_data_stream_event() else {
            unreachable!()
        };

        match (
            data_steam_event.new_watermark,
            &self.appended_new_watermark.unwrap(),
        ) {
            (Some(_), None) => Err(AppendValidationError::WatermarkIsNotMonotonic.into()),
            (Some(prev_wm), Some(next_wm)) if prev_wm > next_wm => {
                Err(AppendValidationError::WatermarkIsNotMonotonic.into())
            }
            _ => Ok(Decision::Stop),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateOffsetsAreSequentialVisitor<'a> {
    appended_block_event: &'a MetadataEvent,
    appended_data_block: Option<MetadataBlockDataStreamRef<'a>>,
}

impl<'a> ValidateOffsetsAreSequentialVisitor<'a> {
    fn validate_internal_offset_consistency(
        event: &MetadataEvent,
        data_block: &MetadataBlockDataStreamRef,
    ) -> Result<(), AppendError> {
        if let Some(new_data) = data_block.event.new_data {
            let expected_start_offset = data_block.event.prev_offset.map_or(0, |v| v + 1);

            if new_data.offset_interval.start != expected_start_offset {
                return Err(AppendValidationError::OffsetsAreNotSequential(
                    OffsetsNotSequentialError::new(
                        expected_start_offset,
                        new_data.offset_interval.start,
                    ),
                )
                .into());
            }

            if new_data.offset_interval.end < new_data.offset_interval.start {
                invalid_event!(event.clone(), "Invalid offset interval");
            }
        }

        Ok(())
    }

    pub fn new(block: &'a MetadataBlock) -> Self {
        Self {
            appended_block_event: &block.event,
            appended_data_block: block.as_data_stream_block(),
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor<'a> {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        if let Some(data_block) = &self.appended_data_block {
            Self::validate_internal_offset_consistency(self.appended_block_event, data_block)?;

            Ok(Decision::NextOfType(Flag::DATA_BLOCK))
        } else {
            Ok(Decision::Stop)
        }
    }

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
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
                )
                .into());
            }

            if new_data.offset_interval.end < new_data.offset_interval.start {
                invalid_event!(self.appended_block_event.clone(), "Invalid offset interval");
            }
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

struct AddDataVisitorState<'a> {
    appended_add_data: &'a AddData,
    prev_schema: Option<SetDataSchema>,
    prev_add_data: Option<AddData>,
    next_block_flags: Flag,
}

struct ExecuteTransformVisitorState<'a> {
    appended_execute_transform: &'a ExecuteTransform,
    prev_transform: Option<SetTransform>,
    prev_schema: Option<SetDataSchema>,
    prev_query: Option<ExecuteTransform>,
    next_block_flags: Flag,
}

enum ValidateLogicalStructureVisitorState<'a> {
    AddData(AddDataVisitorState<'a>),
    ExecuteTransform(ExecuteTransformVisitorState<'a>),
    SetPollingSource {
        appended_set_polling_source: &'a SetPollingSource,
        appended_event: &'a MetadataEvent,
    },
    AddPushSource {
        appended_add_push_source: &'a AddPushSource,
        appended_event: &'a MetadataEvent,
    },
    SetTransform {
        appended_set_transform: &'a SetTransform,
        appended_event: &'a MetadataEvent,
    },
    Stopped,
}

type State<'a> = ValidateLogicalStructureVisitorState<'a>;

pub struct ValidateLogicalStructureVisitor<'a> {
    state: State<'a>,
}

impl<'a> ValidateLogicalStructureVisitor<'a> {
    fn validate_transform(e: &MetadataEvent, transform: &Transform) -> Result<(), AppendError> {
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

    pub fn new(block: &'a MetadataBlock) -> Self {
        let state = match &block.event {
            MetadataEvent::SetDataSchema(_) => {
                // TODO: Consider schema evolution rules
                // TODO: Consider what happens with previously defined sources
                State::Stopped
            }
            MetadataEvent::AddData(e) => State::AddData(AddDataVisitorState {
                appended_add_data: e,
                prev_schema: None,
                prev_add_data: None,
                next_block_flags: Flag::SET_DATA_SCHEMA | Flag::ADD_DATA,
            }),
            // TODO: ensure only used on Derivative datasets
            MetadataEvent::ExecuteTransform(e) => {
                State::ExecuteTransform(ExecuteTransformVisitorState {
                    appended_execute_transform: e,
                    prev_transform: None,
                    prev_schema: None,
                    prev_query: None,
                    next_block_flags: Flag::SET_DATA_SCHEMA
                        | Flag::SET_TRANSFORM
                        | Flag::EXECUTE_TRANSFORM,
                })
            }
            MetadataEvent::SetPollingSource(e) => State::SetPollingSource {
                appended_set_polling_source: e,
                appended_event: &block.event,
            },
            MetadataEvent::DisablePollingSource(_) => {
                // TODO: Ensure has previously active polling source
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::AddPushSource(e) => State::AddPushSource {
                appended_add_push_source: e,
                appended_event: &block.event,
            },
            MetadataEvent::DisablePushSource(_) => {
                // TODO: Ensure has previous push source with matching name
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::SetTransform(e) => State::SetTransform {
                appended_set_transform: e,
                appended_event: &block.event,
            },
            MetadataEvent::Seed(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => State::Stopped,
        };

        Self { state }
    }

    pub fn post_visit(self) -> Result<(), AppendError> {
        match self.state {
            State::AddData(state) => Self::handle_post_visit_add_data(state),
            State::ExecuteTransform(state) => Self::handle_post_visit_execute_transform(state),
            State::SetPollingSource { .. }
            | State::AddPushSource { .. }
            | State::SetTransform { .. }
            | State::Stopped => Ok(()),
        }
    }

    fn handle_post_visit_add_data(
        AddDataVisitorState {
            appended_add_data: e,
            prev_schema,
            prev_add_data,
            ..
        }: AddDataVisitorState,
    ) -> Result<(), AppendError> {
        // Validate schema was defined before adding any data
        if prev_schema.is_none() && e.new_data.is_some() {
            invalid_event!(
                e.clone(),
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
                e.clone(),
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
                e.clone(),
                "Event neither has data nor it advances checkpoint, watermark, or source state",
            )
            .into());
        }

        Ok(())
    }

    fn handle_post_visit_execute_transform(
        ExecuteTransformVisitorState {
            appended_execute_transform: e,
            prev_transform,
            prev_schema,
            prev_query,
            ..
        }: ExecuteTransformVisitorState,
    ) -> Result<(), AppendError> {
        // Validate schema was defined if we're adding data
        if prev_schema.is_none() && e.new_data.is_some() {
            invalid_event!(
                e.clone(),
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
                    e.clone(),
                    "Inputs must be listed in same order as initially declared in SetTransform \
                     event",
                );
            }
        } else if let Some(prev_query) = &prev_query {
            if actual_inputs.ne(prev_query.query_inputs.iter().map(|i| &i.dataset_id)) {
                invalid_event!(
                    e.clone(),
                    "Inputs must be listed in same order as initially declared in SetTransform \
                     event",
                );
            }
        } else {
            invalid_event!(
                e.clone(),
                "ExecuteTransform must be preceded by SetTransform event",
            );
        }

        // Validate input offset and block sequencing
        if let Some(prev_query) = &prev_query {
            for (prev, new) in prev_query.query_inputs.iter().zip(&e.query_inputs) {
                if new.new_block_hash.is_some() && new.new_block_hash == new.prev_block_hash {
                    invalid_event!(e.clone(), "Invalid input block interval");
                }

                if new.new_offset.is_some() && new.new_offset == new.prev_offset {
                    invalid_event!(e.clone(), "Invalid input offset interval");
                }

                if new.prev_block_hash.as_ref() != prev.last_block_hash() {
                    invalid_event!(
                        e.clone(),
                        "Input prevBlockHash does not correspond to the last block included in \
                         the previous query",
                    );
                }

                if new.prev_offset != prev.last_offset() {
                    invalid_event!(
                        e.clone(),
                        "Input prevOffset hash does not correspond to the last offset included in \
                         the previous query",
                    );
                }

                if new.new_offset.is_some() && new.new_block_hash.is_none() {
                    invalid_event!(
                        e.clone(),
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
                e.clone(),
                "Input checkpoint does not correspond to the last checkpoint in the chain",
            );
        }

        // Validate event advances some state
        if e.new_data.is_none()
            && e.new_checkpoint.as_ref().map(|v| &v.physical_hash) == e.prev_checkpoint.as_ref()
            && e.new_watermark.as_ref() == prev_watermark
        {
            return Err(AppendValidationError::no_op_event(
                e.clone(),
                "Event neither has data nor it advances checkpoint or watermark",
            )
            .into());
        }

        Ok(())
    }
}

impl<'a> MetadataChainVisitor for ValidateLogicalStructureVisitor<'a> {
    type Error = AppendError;

    fn initial_decision(&self) -> Result<Decision, Self::Error> {
        match &self.state {
            State::AddData(state) => {
                // TODO: ensure only used on Root datasets
                let e = state.appended_add_data;

                if e.is_empty() {
                    return Err(
                        AppendValidationError::no_op_event(e.clone(), "Event is empty").into(),
                    );
                }

                Ok(Decision::NextOfType(state.next_block_flags))
            }
            State::ExecuteTransform(state) => {
                let e = state.appended_execute_transform;

                if e.is_empty() {
                    return Err(
                        AppendValidationError::no_op_event(e.clone(), "Event is empty").into(),
                    );
                }

                Ok(Decision::NextOfType(state.next_block_flags))
            }
            State::SetPollingSource {
                appended_set_polling_source: e,
                appended_event,
            } => {
                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    Self::validate_transform(appended_event, transform)?;
                }

                // Ensure no active push sources
                Ok(Decision::NextOfType(Flag::ADD_PUSH_SOURCE))
            }
            State::AddPushSource {
                appended_add_push_source: e,
                appended_event,
            } => {
                // Ensure specifies the schema
                if e.read.schema().is_none() {
                    invalid_event!(
                        (*e).clone(),
                        "Push sources must specify the read schema explicitly",
                    );
                }

                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    Self::validate_transform(appended_event, transform)?;
                }

                Ok(Decision::NextOfType(Flag::SET_POLLING_SOURCE))
            }
            State::SetTransform {
                appended_set_transform: e,
                appended_event,
            } => {
                // Ensure has inputs
                if e.inputs.is_empty() {
                    invalid_event!((*e).clone(), "Transform must have at least one input");
                }

                // Ensure inputs are resolved to IDs and aliases are specified
                for i in &e.inputs {
                    if i.dataset_ref.id().is_none() || i.alias.is_none() {
                        invalid_event!(
                            (*e).clone(),
                            "Transform inputs must be resolved to dataset IDs and specify aliases"
                        );
                    }
                }

                // Queries must be normalized
                Self::validate_transform(appended_event, &e.transform)?;

                Ok(Decision::Stop)
            }
            State::Stopped => Ok(Decision::Stop),
        }
    }

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::Error> {
        match &mut self.state {
            State::AddData(state) => {
                match &block.event {
                    MetadataEvent::AddData(e) => {
                        state.prev_add_data = Some(e.clone());
                        state.next_block_flags -= Flag::ADD_DATA;
                    }
                    MetadataEvent::SetDataSchema(e) => {
                        state.prev_schema = Some(e.clone());
                        state.next_block_flags -= Flag::SET_DATA_SCHEMA;
                    }
                    _ => unreachable!(),
                }

                Ok(Decision::NextOfType(state.next_block_flags))
            }
            State::ExecuteTransform(state) => {
                match &block.event {
                    MetadataEvent::SetDataSchema(e) => {
                        state.prev_schema = Some(e.clone());
                        state.next_block_flags -= Flag::SET_DATA_SCHEMA;
                    }
                    MetadataEvent::SetTransform(e) => {
                        state.prev_transform = Some(e.clone());
                        state.next_block_flags -= Flag::SET_TRANSFORM;
                    }
                    MetadataEvent::ExecuteTransform(e) => {
                        state.prev_query = Some(e.clone());
                        state.next_block_flags -= Flag::EXECUTE_TRANSFORM;
                    }
                    _ => unreachable!(),
                }

                // Note: `prev_transform` is optional
                if state.prev_schema.is_some() && state.prev_query.is_some() {
                    state.next_block_flags -= Flag::SET_TRANSFORM;
                }

                Ok(Decision::NextOfType(state.next_block_flags))
            }
            State::SetPollingSource { .. } => {
                let MetadataEvent::AddPushSource(e) = &block.event else {
                    unreachable!()
                };

                invalid_event!(
                    e.clone(),
                    "Cannot add a polling source while some push sources are still active",
                );
            }
            State::AddPushSource { .. } => {
                let MetadataEvent::SetPollingSource(e) = &block.event else {
                    unreachable!()
                };

                invalid_event!(
                    e.clone(),
                    "Cannot add a push source while polling source is still active",
                );
            }
            State::Stopped | State::SetTransform { .. } => {
                unreachable!()
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
