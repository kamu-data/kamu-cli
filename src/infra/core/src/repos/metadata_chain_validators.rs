// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

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

use crate::{
    invalid_event,
    BoxedVisitor,
    BoxedVisitors,
    Decision,
    MetadataBlockTypeFlags,
    MetadataChainVisitor,
};

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
        match &self.initial_block.prev_block_hash {
            Some(hash) => Ok(Decision::NextWithHash(hash.clone())),
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

        match &block.prev_block_hash {
            Some(hash) => Ok(Decision::NextWithHash(hash.clone())),
            None if block.sequence_number != 0 => Err(AppendValidationError::SequenceIntegrity(
                SequenceIntegrityError {
                    prev_block_hash: None,
                    prev_block_sequence_number: None,
                    next_block_sequence_number: block.sequence_number,
                },
            )
            .into()),
            None => Ok(Decision::Stop),
        }
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
        if let Some(hash) = &self.initial_block.prev_block_hash {
            Ok(Decision::NextWithHash(hash.clone()))
        } else {
            Ok(Decision::Stop)
        }
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

type DecisionMap<'a> = HashMap<Decision, Vec<BoxedVisitor<'a>>>;

type RequestedByHashVisitors<'a> = Vec<(Multihash, BoxedVisitors<'a>)>;
type RequestedByFlagsVisitors<'a> = Vec<(MetadataBlockTypeFlags, BoxedVisitors<'a>)>;

pub struct MetadataChainVisitorBatchProcessor {}

impl MetadataChainVisitorBatchProcessor {
    pub fn process_decision_map(
        visitors_count: usize,
        decision_map: DecisionMap,
    ) -> Option<(RequestedByHashVisitors, RequestedByFlagsVisitors)> {
        let mut requested_by_hash_visitors = Vec::new();
        let mut requested_by_flags_visitors = Vec::new();

        for (decision, visitors) in decision_map {
            match decision {
                Decision::Stop if visitors.len() == visitors_count => {
                    // All Visitors have finished
                    return None;
                }
                Decision::Stop => {
                    // Some Visitors have already finished,
                    // we are processing rest
                }
                Decision::NextWithHash(hash) => {
                    requested_by_hash_visitors.push((hash, visitors));
                }
                Decision::NextOfType(flags) => {
                    requested_by_flags_visitors.push((flags, visitors));
                }
            }
        }

        Some((requested_by_hash_visitors, requested_by_flags_visitors))
    }

    pub fn get_next_decisions(visitors: BoxedVisitors) -> Result<DecisionMap, AppendError> {
        visitors.into_iter().try_fold(
            HashMap::<Decision, BoxedVisitors>::new(),
            |mut acc, mut visitor| {
                let decision = visitor.visit()?;

                acc.entry(decision).or_default().push(visitor);

                Ok(acc)
            },
        )
    }

    fn get_next_decisions_with_block_impl<'a>(
        visitors: BoxedVisitors<'a>,
        hashed_block: HashedMetadataBlockRef,
    ) -> Result<DecisionMap<'a>, AppendError> {
        visitors.into_iter().try_fold(
            HashMap::<Decision, BoxedVisitors>::new(),
            |mut acc, mut visitor| {
                let decision = visitor.visit_with_block(hashed_block)?;

                acc.entry(decision).or_default().push(visitor);

                Ok(acc)
            },
        )
    }

    pub fn get_next_decisions_with_block<'a>(
        visitors: BoxedVisitors<'a>,
        hashed_block: HashedMetadataBlockRef,
        prev_decision_map: DecisionMap<'a>,
    ) -> Result<DecisionMap<'a>, AppendError> {
        let map = MetadataChainVisitorBatchProcessor::get_next_decisions_with_block_impl(
            visitors,
            hashed_block,
        )?;

        Ok(Self::merge_maps(prev_decision_map, map))
    }

    fn merge_maps<'a>(
        mut left_map: DecisionMap<'a>,
        right_map: DecisionMap<'a>,
    ) -> DecisionMap<'a> {
        for (decision, visitors) in right_map {
            match left_map.entry(decision) {
                Entry::Occupied(entry) => entry.into_mut().extend(visitors),
                Entry::Vacant(entry) => {
                    entry.insert(visitors);
                }
            }
        }

        left_map
    }
}
