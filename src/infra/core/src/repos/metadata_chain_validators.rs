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
    OffsetsNotSequentialError,
    SequenceIntegrityError,
};
use opendatafabric::{DataSlice, IntoDataStreamBlock, MetadataBlock, MetadataEvent, Multihash};

use crate::{
    invalid_event,
    BoxedVisitors,
    Decision,
    MetadataBlockTypeFlags,
    MetadataBlockWithOptionalHashRef,
    MetadataChainVisitor,
};

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ValidateSeedBlockOrderVisitor {}

impl MetadataChainVisitor for ValidateSeedBlockOrderVisitor {
    fn visit(
        &mut self,
        (block, _): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
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
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ValidatePrevBlockExistsVisitor {
    next_prev_block_hash: Option<Multihash>,
}

impl MetadataChainVisitor for ValidatePrevBlockExistsVisitor {
    fn visit(
        &mut self,
        (block, maybe_hash): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
        let Some(next_prev_block_hash) = &self.next_prev_block_hash else {
            self.next_prev_block_hash = block.prev_block_hash.clone();

            return Ok(Decision::Next);
        };

        if next_prev_block_hash != maybe_hash.unwrap() {
            return Err(
                AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                    hash: next_prev_block_hash.clone(),
                })
                .into(),
            );
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ValidateSequenceNumbersIntegrityVisitor {
    next_sequence_number: Option<u64>,
}

impl ValidateSequenceNumbersIntegrityVisitor {
    fn process_first_block(
        &mut self,
        first_block: &MetadataBlock,
    ) -> Result<Decision, AppendError> {
        self.next_sequence_number = Some(first_block.sequence_number);

        if first_block.prev_block_hash.is_none() && first_block.sequence_number != 0 {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: first_block.sequence_number,
                })
                .into(),
            );
        }

        Ok(Decision::Next)
    }
}

impl MetadataChainVisitor for ValidateSequenceNumbersIntegrityVisitor {
    fn visit(
        &mut self,
        (block, maybe_hash): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
        let Some(next_sequence_number) = self.next_sequence_number else {
            return self.process_first_block(block);
        };

        if block.sequence_number != (next_sequence_number - 1) {
            return Err(
                AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                    prev_block_hash: Some(maybe_hash.unwrap().clone()),
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

#[derive(Default)]
pub struct ValidateSystemTimeIsMonotonicVisitor {
    next_system_time: Option<DateTime<Utc>>,
}

impl MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor {
    fn visit(
        &mut self,
        (block, _): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
        let Some(next_system_time) = self.next_system_time else {
            self.next_system_time = Some(block.system_time);

            return Ok(Decision::Next);
        };

        if next_system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ValidateWatermarkIsMonotonicVisitor {
    has_first_block_processed: bool,
    next_new_watermark: Option<DateTime<Utc>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    fn process_first_block(&mut self, first_block: &MetadataBlock) -> Decision {
        self.has_first_block_processed = true;

        let Some(data_block) = first_block.as_data_stream_block() else {
            // If it's not a data block, there's nothing to validate
            return Decision::Stop;
        };

        self.next_new_watermark = data_block.event.new_watermark.copied();

        Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK)
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    fn visit(
        &mut self,
        (block, _): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
        if !self.has_first_block_processed {
            return Ok(self.process_first_block(block));
        };

        let Some(data_block) = block.as_data_stream_block() else {
            return Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK));
        };

        match (data_block.event.new_watermark, &self.next_new_watermark) {
            (Some(_), None) => Err(AppendValidationError::WatermarkIsNotMonotonic.into()),
            (Some(prev_wm), Some(next_wm)) if prev_wm > next_wm => {
                Err(AppendValidationError::WatermarkIsNotMonotonic.into())
            }
            _ => Ok(Decision::Stop),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ValidateOffsetsAreSequentialVisitor {
    has_first_block_processed: bool,
    next_prev_offset: Option<u64>,
    next_new_data: Option<DataSlice>,
    next_data_event: Option<MetadataEvent>,
}

impl ValidateOffsetsAreSequentialVisitor {
    fn process_first_block(&mut self, first_block: &MetadataBlock) -> Decision {
        self.has_first_block_processed = true;

        let Some(data_block) = first_block.as_data_stream_block() else {
            // If it's not a data block, there's nothing to validate
            return Decision::Stop;
        };

        self.next_prev_offset = data_block.event.prev_offset;
        self.next_new_data = data_block.event.new_data.cloned();
        self.next_data_event = Some(first_block.event.clone());

        Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK)
    }
}

impl MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor {
    fn visit(
        &mut self,
        (block, _): MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError> {
        // Only check AddData and ExecuteTransform.
        // SetWatermark is also considered a data stream event but does not carry the
        // offsets.
        if !self.has_first_block_processed {
            return Ok(self.process_first_block(block));
        };

        let Some(data_block) = block.as_data_stream_block() else {
            return Ok(Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK));
        };

        // Validate input/output offset sequencing
        let expected_next_prev_offset = data_block.event.last_offset();

        if self.next_prev_offset != expected_next_prev_offset {
            invalid_event!(
                self.next_data_event.take().unwrap(),
                "Carried prev offset does not correspond to the last offset in the chain",
            );
        }

        // Validate internal offset consistency
        if let Some(new_data) = &self.next_new_data {
            let expected_start_offset = self.next_prev_offset.map_or(0, |v| v + 1);

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
                invalid_event!(
                    self.next_data_event.take().unwrap(),
                    "Invalid offset interval",
                );
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
    pub fn get_next_decisions(
        visitors: BoxedVisitors,
        block_with_optional_hash: MetadataBlockWithOptionalHashRef,
    ) -> Result<(BoxedVisitors, Decision), AppendError> {
        let acc_capacity = visitors.len();

        visitors.into_iter().try_fold(
            (Vec::with_capacity(acc_capacity), Decision::Stop),
            |mut acc, mut visitor| {
                let decision = visitor.visit(block_with_optional_hash)?;

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
