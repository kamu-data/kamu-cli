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
    OffsetsNotSequentialError,
    SequenceIntegrityError,
};
use opendatafabric::{
    IntoDataStreamBlock,
    IntoDataStreamEvent,
    MetadataBlock,
    MetadataBlockDataStreamRef,
    MetadataEvent,
    Multihash,
};

use crate::{invalid_event, Decision, MetadataBlockTypeFlags, MetadataChainVisitor};

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSeedBlockOrderVisitor {}

impl ValidateSeedBlockOrderVisitor {
    pub fn new(block: &MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let decision = match block.event {
            MetadataEvent::Seed(_) if block.prev_block_hash.is_some() => {
                Err(AppendValidationError::AppendingSeedBlockToNonEmptyChain.into())
            }
            MetadataEvent::Seed(_) => Ok(Decision::Stop),
            _ if block.prev_block_hash.is_none() => {
                Err(AppendValidationError::FirstBlockMustBeSeed.into())
            }
            _ => Result::<Decision, AppendError>::Ok(Decision::Stop),
        }?;

        Ok((decision, Self {}))
    }
}

impl MetadataChainVisitor for ValidateSeedBlockOrderVisitor {
    type VisitError = AppendError;

    fn visit(&mut self, _: HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidatePrevBlockExistsVisitor<'a> {
    initial_prev_block_hash: Option<&'a Multihash>,
}

impl<'a> ValidatePrevBlockExistsVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let decision = if block.prev_block_hash.is_some() {
            Decision::Next
        } else {
            Decision::Stop
        };

        Ok((
            decision,
            Self {
                initial_prev_block_hash: block.prev_block_hash.as_ref(),
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidatePrevBlockExistsVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (hash, _): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        let Some(initial_prev_block_hash) = self.initial_prev_block_hash else {
            unreachable!()
        };

        if initial_prev_block_hash != hash {
            return Err(
                AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                    hash: initial_prev_block_hash.clone(),
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSequenceNumbersIntegrityVisitor {
    initial_sequence_number: u64,
}

impl<'a> ValidateSequenceNumbersIntegrityVisitor {
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let decision = match &block.prev_block_hash {
            Some(_) => Result::<Decision, AppendError>::Ok(Decision::Next),
            None if block.sequence_number != 0 => Err(AppendValidationError::SequenceIntegrity(
                SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: block.sequence_number,
                },
            )
            .into()),
            None => Ok(Decision::Stop),
        }?;

        Ok((
            decision,
            Self {
                initial_sequence_number: block.sequence_number,
            },
        ))
    }
}

impl MetadataChainVisitor for ValidateSequenceNumbersIntegrityVisitor {
    type VisitError = AppendError;

    fn visit(
        &mut self,
        (hash, block): HashedMetadataBlockRef,
    ) -> Result<Decision, Self::VisitError> {
        if block.sequence_number != (self.initial_sequence_number - 1) {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: Some(hash.clone()),
                    prev_block_sequence_number: Some(block.sequence_number),
                    next_block_sequence_number: self.initial_sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSystemTimeIsMonotonicVisitor<'a> {
    initial_system_time: &'a DateTime<Utc>,
}

impl<'a> ValidateSystemTimeIsMonotonicVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let decision = if block.prev_block_hash.is_some() {
            Decision::Next
        } else {
            Decision::Stop
        };

        Ok((
            decision,
            Self {
                initial_system_time: &block.system_time,
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        if *self.initial_system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateWatermarkIsMonotonicVisitor {
    initial_new_watermark: Option<DateTime<Utc>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    pub fn new(block: &MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let (decision, initial_new_watermark) =
            if let Some(data_steam_event) = block.event.as_data_stream_event() {
                (
                    Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK),
                    data_steam_event.new_watermark.copied(),
                )
            } else {
                (Decision::Stop, None)
            };

        Ok((
            decision,
            Self {
                initial_new_watermark,
            },
        ))
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        let Some(data_steam_event) = block.event.as_data_stream_event() else {
            return Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK));
        };

        match (data_steam_event.new_watermark, &self.initial_new_watermark) {
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
    initial_block_event: &'a MetadataEvent,
    initial_data_block: Option<MetadataBlockDataStreamRef<'a>>,
}

impl<'a> ValidateOffsetsAreSequentialVisitor<'a> {
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let initial_data_block = block.as_data_stream_block();
        let decision = if initial_data_block.is_some() {
            Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK)
        } else {
            Decision::Stop
        };

        Ok((
            decision,
            Self {
                initial_block_event: &block.event,
                initial_data_block,
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        // Only check AddData and ExecuteTransform.
        // SetWatermark is also considered a data stream event but does not carry the
        // offsets.
        let Some(data_block) = block.as_data_stream_block() else {
            return Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK));
        };

        let Some(initial_data_block) = &self.initial_data_block else {
            unreachable!()
        };

        // Validate input/output offset sequencing
        let expected_next_prev_offset = data_block.event.last_offset();

        if initial_data_block.event.prev_offset != expected_next_prev_offset {
            invalid_event!(
                self.initial_block_event.clone(),
                "Carried prev offset does not correspond to the last offset in the chain",
            );
        }

        // Validate internal offset consistency
        if let Some(new_data) = initial_data_block.event.new_data {
            let expected_start_offset = initial_data_block.event.prev_offset.map_or(0, |v| v + 1);

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
                invalid_event!(self.initial_block_event.clone(), "Invalid offset interval",);
            }
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////
