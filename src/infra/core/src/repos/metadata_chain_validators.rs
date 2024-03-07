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
    Decision,
    HashedMetadataBlockRef,
    MetadataBlockTypeFlags,
    MetadataChainVisitor,
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

use crate::invalid_event;

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
    appended_prev_block_hash: Option<&'a Multihash>,
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
                appended_prev_block_hash: block.prev_block_hash.as_ref(),
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidatePrevBlockExistsVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (hash, _): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
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
}

impl<'a> ValidateSequenceNumbersIntegrityVisitor {
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
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

        Ok((
            Decision::Next,
            Self {
                appended_sequence_number: block.sequence_number,
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
    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        Ok((
            Decision::Next,
            Self {
                appended_system_time: &block.system_time,
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidateSystemTimeIsMonotonicVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        if *self.appended_system_time < block.system_time {
            return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
        }

        Ok(Decision::Stop)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct ValidateWatermarkIsMonotonicVisitor {
    appended_new_watermark: Option<DateTime<Utc>>,
}

impl ValidateWatermarkIsMonotonicVisitor {
    pub fn new(block: &MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let (decision, appended_new_watermark) =
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
                appended_new_watermark,
            },
        ))
    }
}

impl MetadataChainVisitor for ValidateWatermarkIsMonotonicVisitor {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
        let Some(data_steam_event) = block.event.as_data_stream_event() else {
            unreachable!()
        };

        match (data_steam_event.new_watermark, &self.appended_new_watermark) {
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

    pub fn new(block: &'a MetadataBlock) -> Result<(Decision, Self), AppendError> {
        let maybe_data_block = block.as_data_stream_block();
        let decision = if let Some(data_block) = &maybe_data_block {
            Self::validate_internal_offset_consistency(&block.event, data_block)?;

            Decision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK)
        } else {
            Decision::Stop
        };

        Ok((
            decision,
            Self {
                appended_block_event: &block.event,
                appended_data_block: maybe_data_block,
            },
        ))
    }
}

impl<'a> MetadataChainVisitor for ValidateOffsetsAreSequentialVisitor<'a> {
    type VisitError = AppendError;

    fn visit(&mut self, (_, block): HashedMetadataBlockRef) -> Result<Decision, Self::VisitError> {
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
