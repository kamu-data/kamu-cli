// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;

use crate::dtos::*;
use crate::formats::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// Same as [MetadataBlock] struct but holds a specific variant of the
/// [MetadataEvent]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlockTyped<T> {
    /// System time when this block was written.
    pub system_time: DateTime<Utc>,
    /// Hash sum of the preceding block.
    pub prev_block_hash: Option<Multihash>,
    /// Block sequence number starting from tail to head.
    pub sequence_number: i32,
    /// Event data.
    pub event: T,
}

pub struct MetadataBlockTypedRef<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub sequence_number: i32,
    pub event: &'a T,
}

pub struct MetadataBlockTypedRefMut<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub sequence_number: i32,
    pub event: &'a mut T,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<T> Into<MetadataBlock> for MetadataBlockTyped<T>
where
    T: Into<MetadataEvent>,
{
    fn into(self) -> MetadataBlock {
        MetadataBlock {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash,
            sequence_number: self.sequence_number,
            event: self.event.into(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Allows "casting" a generic MetadataBlock into one containing a specific
/// event type
pub trait AsTypedBlock {
    fn into_typed<T: VariantOf<MetadataEvent>>(self) -> Option<MetadataBlockTyped<T>>;
    fn as_typed<'a, T: VariantOf<MetadataEvent>>(&'a self) -> Option<MetadataBlockTypedRef<'a, T>>;
    fn as_typed_mut<'a, T: VariantOf<MetadataEvent>>(
        &'a mut self,
    ) -> Option<MetadataBlockTypedRefMut<'a, T>>;
}

impl AsTypedBlock for MetadataBlock {
    fn into_typed<T: VariantOf<MetadataEvent>>(self) -> Option<MetadataBlockTyped<T>> {
        T::into_variant(self.event).map(|e| MetadataBlockTyped {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash,
            sequence_number: self.sequence_number,
            event: e,
        })
    }

    fn as_typed<'a, T: VariantOf<MetadataEvent>>(&'a self) -> Option<MetadataBlockTypedRef<'a, T>> {
        T::as_variant(&self.event).map(|e| MetadataBlockTypedRef {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            sequence_number: self.sequence_number,
            event: e,
        })
    }

    fn as_typed_mut<'a, T: VariantOf<MetadataEvent>>(
        &'a mut self,
    ) -> Option<MetadataBlockTypedRefMut<'a, T>> {
        T::as_variant_mut(&mut self.event).map(|e| MetadataBlockTypedRefMut {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            sequence_number: self.sequence_number,
            event: e,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataEventDataStream {
    pub output_data: Option<DataSlice>,
    pub output_checkpoint: Option<Checkpoint>,
    pub output_watermark: Option<DateTime<Utc>>,
}

pub struct MetadataEventDataStreamRef<'a> {
    pub output_data: Option<&'a DataSlice>,
    pub output_checkpoint: Option<&'a Checkpoint>,
    pub output_watermark: Option<&'a DateTime<Utc>>,
}

pub trait IntoDataStreamEvent {
    fn into_data_stream_event(self) -> Option<MetadataEventDataStream>;
    fn as_data_stream_event<'a>(&'a self) -> Option<MetadataEventDataStreamRef<'a>>;
}

impl IntoDataStreamEvent for MetadataEvent {
    fn into_data_stream_event(self) -> Option<MetadataEventDataStream> {
        let (output_data, output_checkpoint, output_watermark) = match self {
            MetadataEvent::AddData(e) => (e.output_data, e.output_checkpoint, e.output_watermark),
            MetadataEvent::ExecuteQuery(e) => {
                (e.output_data, e.output_checkpoint, e.output_watermark)
            }
            MetadataEvent::SetWatermark(e) => (None, None, Some(e.output_watermark)),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_) => return None,
        };
        Some(MetadataEventDataStream {
            output_data,
            output_checkpoint,
            output_watermark,
        })
    }

    fn as_data_stream_event<'a>(&'a self) -> Option<MetadataEventDataStreamRef<'a>> {
        let (output_data, output_checkpoint, output_watermark) = match &self {
            MetadataEvent::AddData(e) => (
                e.output_data.as_ref(),
                e.output_checkpoint.as_ref(),
                e.output_watermark.as_ref(),
            ),
            MetadataEvent::ExecuteQuery(e) => (
                e.output_data.as_ref(),
                e.output_checkpoint.as_ref(),
                e.output_watermark.as_ref(),
            ),
            MetadataEvent::SetWatermark(e) => (None, None, Some(&e.output_watermark)),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_) => return None,
        };
        Some(MetadataEventDataStreamRef {
            output_data,
            output_checkpoint,
            output_watermark,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockDataStream {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub event: MetadataEventDataStream,
}

pub struct MetadataBlockDataStreamRef<'a> {
    pub system_time: &'a DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub event: MetadataEventDataStreamRef<'a>,
}

pub trait IntoDataStreamBlock {
    fn into_data_stream_block(self) -> Option<MetadataBlockDataStream>;
    fn as_data_stream_block<'a>(&'a self) -> Option<MetadataBlockDataStreamRef<'a>>;
}

impl IntoDataStreamBlock for MetadataBlock {
    fn into_data_stream_block(self) -> Option<MetadataBlockDataStream> {
        if let Some(event) = self.event.into_data_stream_event() {
            Some(MetadataBlockDataStream {
                system_time: self.system_time,
                prev_block_hash: self.prev_block_hash,
                event,
            })
        } else {
            None
        }
    }

    fn as_data_stream_block<'a>(&'a self) -> Option<MetadataBlockDataStreamRef<'a>> {
        if let Some(event) = self.event.as_data_stream_event() {
            Some(MetadataBlockDataStreamRef {
                system_time: &self.system_time,
                prev_block_hash: self.prev_block_hash.as_ref(),
                event,
            })
        } else {
            None
        }
    }
}
