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

use crate::{invalid_event, BoxedVisitors, Decision, MetadataBlockTypeFlags, MetadataChainVisitor};

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSeedBlockOrderVisitor<'a> {
    initial_block: &'a MetadataBlock,
}

impl<'a> ValidateSeedBlockOrderVisitor<'a> {
    pub fn new((_, block): HashedMetadataBlockRef<'a>) -> Self {
        Self {
            initial_block: block,
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateSeedBlockOrderVisitor<'a> {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        let block = self.initial_block;

        match block.event {
            MetadataEvent::Seed(_) if block.prev_block_hash.is_some() => {
                Err(AppendValidationError::AppendingSeedBlockToNonEmptyChain.into())
            }
            MetadataEvent::Seed(_) => Ok(Decision::Stop),
            _ if block.prev_block_hash.is_none() => {
                Err(AppendValidationError::FirstBlockMustBeSeed.into())
            }
            _ => Ok(Decision::Stop),
        }
    }

    fn visit_with_block(&mut self, _: HashedMetadataBlockRef) -> Result<Decision, AppendError> {
        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidatePrevBlockExistsVisitor<'a> {
    initial_hash: &'a Multihash,
    initial_block: &'a MetadataBlock,
}

impl<'a> ValidatePrevBlockExistsVisitor<'a> {
    pub fn new((hash, block): HashedMetadataBlockRef<'a>) -> Self {
        Self {
            initial_hash: hash,
            initial_block: block,
        }
    }
}

impl<'a> MetadataChainVisitor for ValidatePrevBlockExistsVisitor<'a> {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        match self.initial_block.prev_block_hash {
            // TODO Use Decision::NextWithHash()
            Some(_) => Ok(Decision::Next),
            None => Ok(Decision::Stop),
        }
    }

    fn visit_with_block(
        &mut self,
        (hash, _): HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError> {
        let Some(initial_prev_block_hash) = &self.initial_block.prev_block_hash else {
            unreachable!()
        };

        if initial_prev_block_hash != hash {
            return Err(
                AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                    hash: self.initial_hash.clone(),
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSequenceNumbersIntegrityVisitor<'a> {
    initial_block: &'a MetadataBlock,
}

impl<'a> ValidateSequenceNumbersIntegrityVisitor<'a> {
    pub fn new((_, block): HashedMetadataBlockRef<'a>) -> Self {
        Self {
            initial_block: block,
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateSequenceNumbersIntegrityVisitor<'a> {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        let block = self.initial_block;

        if block.prev_block_hash.is_none() && block.sequence_number != 0 {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: block.sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Next)
    }

    fn visit_with_block(
        &mut self,
        (hash, block): HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError> {
        let next_sequence_number = self.initial_block.sequence_number;

        if block.sequence_number != (next_sequence_number - 1) {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: Some(hash.clone()),
                    prev_block_sequence_number: Some(block.sequence_number),
                    next_block_sequence_number: next_sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateSystemTimeIsMonotonicVisitor<'a> {
    initial_block: &'a MetadataBlock,
}

impl<'a> ValidateSystemTimeIsMonotonicVisitor<'a> {
    pub fn new((_, block): HashedMetadataBlockRef<'a>) -> Self {
        Self {
            initial_block: block,
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor<'a> {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        Ok(Decision::Next)
    }

    fn visit_with_block(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError> {
        if self.initial_block.system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateWatermarkIsMonotonicVisitor {
    is_initial_data_event: bool,
    initial_new_watermark: Option<DateTime<Utc>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    pub fn new((_, block): HashedMetadataBlockRef) -> Self {
        let (is_initial_data_event, initial_new_watermark) =
            if let Some(data_steam_event) = block.event.as_data_stream_event() {
                (true, data_steam_event.new_watermark.copied())
            } else {
                (false, None)
            };

        Self {
            is_initial_data_event,
            initial_new_watermark,
        }
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        if !self.is_initial_data_event {
            // If it's not a data block, there's nothing to validate
            return Ok(Decision::Stop);
        };

        Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK))
    }

    fn visit_with_block(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError> {
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
    initial_block: &'a MetadataBlock,
    initial_data_block: Option<MetadataBlockDataStreamRef<'a>>,
}

impl<'a> ValidateOffsetsAreSequentialVisitor<'a> {
    pub fn new((_, block): HashedMetadataBlockRef<'a>) -> Self {
        Self {
            initial_block: block,
            initial_data_block: block.as_data_stream_block(),
        }
    }
}

impl<'a> MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor<'a> {
    fn visit(&mut self) -> Result<Decision, AppendError> {
        if self.initial_data_block.is_none() {
            // If it's not a data block, there's nothing to validate
            return Ok(Decision::Stop);
        };

        Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK))
    }

    fn visit_with_block(
        &mut self,
        (_, block): HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError> {
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
                self.initial_block.event.clone(),
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
                invalid_event!(self.initial_block.event.clone(), "Invalid offset interval",);
            }
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainVisitorBatchProcessor {}

impl MetadataChainVisitorBatchProcessor {
    // TODO: extract common part
    pub fn get_next_decisions(
        visitors: BoxedVisitors,
    ) -> Result<(BoxedVisitors, Decision), AppendError> {
        let acc_capacity = visitors.len();

        visitors.into_iter().try_fold(
            (Vec::with_capacity(acc_capacity), Decision::Stop),
            |mut acc, mut visitor| {
                let decision = visitor.visit()?;

                if decision != Decision::Stop {
                    acc.0.push(visitor);
                    acc.1 = Self::apply_decision(acc.1, decision);
                }

                Ok(acc)
            },
        )
    }

    pub fn get_next_decisions_with_block<'a>(
        visitors: BoxedVisitors<'a>,
        hashed_block: HashedMetadataBlockRef,
    ) -> Result<(BoxedVisitors<'a>, Decision), AppendError> {
        let acc_capacity = visitors.len();

        visitors.into_iter().try_fold(
            (Vec::with_capacity(acc_capacity), Decision::Stop),
            |mut acc, mut visitor| {
                let decision = visitor.visit_with_block(hashed_block)?;

                if decision != Decision::Stop {
                    acc.0.push(visitor);
                    acc.1 = Self::apply_decision(acc.1, decision);
                }

                Ok(acc)
            },
        )
    }

    fn apply_decision(left_decision: Decision, right_decision: Decision) -> Decision {
        // Sorting helps us eliminate duplicate pairs in the following comparison
        let decision_pair = if left_decision > right_decision {
            (right_decision, left_decision)
        } else {
            (left_decision, right_decision)
        };

        match decision_pair {
            (Decision::Stop, Decision::Stop) => Decision::Stop,
            (Decision::Stop, non_stop_decision) => non_stop_decision,
            (Decision::Next, _) => Decision::Next,
            (Decision::NextWithHash(_), Decision::NextWithHash(_)) => {
                // TODO: Discuss at the review
                panic!("Ambiguity in the choice of decision")
            }
            (Decision::NextWithHash(_), next_of_type @ Decision::NextOfType(_)) => next_of_type,
            (Decision::NextOfType(left_flags), Decision::NextOfType(right_flags)) => {
                Decision::NextOfType(left_flags | right_flags)
            }
            _ => {
                unreachable!()
            }
        }
    }
}
