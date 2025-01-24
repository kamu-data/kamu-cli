// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use enum_variants::*;
use internal_error::{InternalError, ResultIntoInternal};

use crate::dtos::*;
use crate::formats::*;
use crate::DatasetRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Same as [`MetadataBlock`] struct but holds a specific variant of the
/// [`MetadataEvent`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlockTyped<T> {
    /// System time when this block was written.
    pub system_time: DateTime<Utc>,
    /// Hash sum of the preceding block.
    pub prev_block_hash: Option<Multihash>,
    /// Block sequence number starting from tail to head.
    pub sequence_number: u64,
    /// Event data.
    pub event: T,
}

pub struct MetadataBlockTypedRef<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub sequence_number: u64,
    pub event: &'a T,
}

pub struct MetadataBlockTypedRefMut<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub sequence_number: u64,
    pub event: &'a mut T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<T> From<MetadataBlockTyped<T>> for MetadataBlock
where
    T: Into<MetadataEvent>,
{
    fn from(val: MetadataBlockTyped<T>) -> Self {
        MetadataBlock {
            system_time: val.system_time,
            prev_block_hash: val.prev_block_hash,
            sequence_number: val.sequence_number,
            event: val.event.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Allows "casting" a generic `MetadataBlock` into one containing a specific
/// event type
pub trait AsTypedBlock {
    fn into_typed<T: VariantOf<MetadataEvent>>(self) -> Option<MetadataBlockTyped<T>>;
    fn as_typed<T: VariantOf<MetadataEvent>>(&self) -> Option<MetadataBlockTypedRef<'_, T>>;
    fn as_typed_mut<T: VariantOf<MetadataEvent>>(
        &mut self,
    ) -> Option<MetadataBlockTypedRefMut<'_, T>>;
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

    fn as_typed<T: VariantOf<MetadataEvent>>(&self) -> Option<MetadataBlockTypedRef<'_, T>> {
        T::as_variant(&self.event).map(|e| MetadataBlockTypedRef {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            sequence_number: self.sequence_number,
            event: e,
        })
    }

    fn as_typed_mut<T: VariantOf<MetadataEvent>>(
        &mut self,
    ) -> Option<MetadataBlockTypedRefMut<'_, T>> {
        T::as_variant_mut(&mut self.event).map(|e| MetadataBlockTypedRefMut {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            sequence_number: self.sequence_number,
            event: e,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataEventDataStream {
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<DataSlice>,
    pub new_checkpoint: Option<Checkpoint>,
    pub new_watermark: Option<DateTime<Utc>>,
}

pub struct MetadataEventDataStreamRef<'a> {
    pub prev_checkpoint: Option<&'a Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<&'a DataSlice>,
    pub new_checkpoint: Option<&'a Checkpoint>,
    pub new_watermark: Option<&'a DateTime<Utc>>,
}

impl MetadataEventDataStream {
    /// Helper for determining the last record offset in the dataset
    pub fn last_offset(&self) -> Option<u64> {
        self.new_data
            .as_ref()
            .map(|d| d.offset_interval.end)
            .or(self.prev_offset)
    }
}

impl MetadataEventDataStreamRef<'_> {
    /// Helper for determining the last record offset in the dataset
    pub fn last_offset(&self) -> Option<u64> {
        self.new_data
            .map(|d| d.offset_interval.end)
            .or(self.prev_offset)
    }
}

pub trait IntoDataStreamEvent {
    fn into_data_stream_event(self) -> Option<MetadataEventDataStream>;
    fn as_data_stream_event(&self) -> Option<MetadataEventDataStreamRef<'_>>;
}

impl IntoDataStreamEvent for MetadataEvent {
    fn into_data_stream_event(self) -> Option<MetadataEventDataStream> {
        let (prev_checkpoint, prev_offset, new_data, new_checkpoint, new_watermark) = match self {
            MetadataEvent::AddData(e) => (
                e.prev_checkpoint,
                e.prev_offset,
                e.new_data,
                e.new_checkpoint,
                e.new_watermark,
            ),
            MetadataEvent::ExecuteTransform(e) => (
                e.prev_checkpoint,
                e.prev_offset,
                e.new_data,
                e.new_checkpoint,
                e.new_watermark,
            ),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::DisablePollingSource(_)
            | MetadataEvent::AddPushSource(_)
            | MetadataEvent::DisablePushSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetDataSchema(_) => return None,
        };
        Some(MetadataEventDataStream {
            prev_checkpoint,
            prev_offset,
            new_data,
            new_checkpoint,
            new_watermark,
        })
    }

    fn as_data_stream_event(&self) -> Option<MetadataEventDataStreamRef<'_>> {
        let (prev_checkpoint, prev_offset, new_data, new_checkpoint, new_watermark) = match &self {
            MetadataEvent::AddData(e) => (
                e.prev_checkpoint.as_ref(),
                e.prev_offset,
                e.new_data.as_ref(),
                e.new_checkpoint.as_ref(),
                e.new_watermark.as_ref(),
            ),
            MetadataEvent::ExecuteTransform(e) => (
                e.prev_checkpoint.as_ref(),
                e.prev_offset,
                e.new_data.as_ref(),
                e.new_checkpoint.as_ref(),
                e.new_watermark.as_ref(),
            ),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::DisablePollingSource(_)
            | MetadataEvent::AddPushSource(_)
            | MetadataEvent::DisablePushSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetDataSchema(_) => return None,
        };
        Some(MetadataEventDataStreamRef {
            prev_checkpoint,
            prev_offset,
            new_data,
            new_checkpoint,
            new_watermark,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    fn as_data_stream_block(&self) -> Option<MetadataBlockDataStreamRef<'_>>;
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

    fn as_data_stream_block(&self) -> Option<MetadataBlockDataStreamRef<'_>> {
        self.event
            .as_data_stream_event()
            .map(|event| MetadataBlockDataStreamRef {
                system_time: &self.system_time,
                prev_block_hash: self.prev_block_hash.as_ref(),
                event,
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait TransformInputExt {
    /// To prevent the dataset ID from leaking, use an alias, if available
    fn into_sanitized_dataset_ref(self) -> Result<DatasetRef, InternalError>;

    /// To prevent the dataset ID from leaking, use an alias, if available
    fn as_sanitized_dataset_ref(&self) -> Result<DatasetRef, InternalError>;
}

impl TransformInputExt for TransformInput {
    fn into_sanitized_dataset_ref(self) -> Result<DatasetRef, InternalError> {
        if let Some(alias) = self.alias {
            DatasetRef::from_str(&alias).int_err()
        } else {
            Ok(self.dataset_ref)
        }
    }

    fn as_sanitized_dataset_ref(&self) -> Result<DatasetRef, InternalError> {
        if let Some(alias) = &self.alias {
            DatasetRef::from_str(alias).int_err()
        } else {
            Ok(self.dataset_ref.clone())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
