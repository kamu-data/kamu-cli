// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockTyped<T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub event: T,
}

pub struct MetadataBlockTypedRef<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub event: &'a T,
}

pub struct MetadataBlockTypedRefMut<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub event: &'a mut T,
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Allows "casting" a generic MetadataBlock into one containing a specific event type
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
            event: e,
        })
    }

    fn as_typed<'a, T: VariantOf<MetadataEvent>>(&'a self) -> Option<MetadataBlockTypedRef<'a, T>> {
        T::as_variant(&self.event).map(|e| MetadataBlockTypedRef {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            event: e,
        })
    }

    fn as_typed_mut<'a, T: VariantOf<MetadataEvent>>(
        &'a mut self,
    ) -> Option<MetadataBlockTypedRefMut<'a, T>> {
        T::as_variant_mut(&mut self.event).map(|e| MetadataBlockTypedRefMut {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            event: e,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub trait EnumWithVariants<E> {
    fn is_variant<V: VariantOf<E>>(&self) -> bool;
    fn into_variant<V: VariantOf<E>>(self) -> Option<V>;
    fn as_variant<V: VariantOf<E>>(&self) -> Option<&V>;
    fn as_variant_mut<V: VariantOf<E>>(&mut self) -> Option<&mut V>;
}

impl EnumWithVariants<MetadataEvent> for MetadataEvent {
    fn is_variant<V: VariantOf<Self>>(&self) -> bool {
        V::is_variant(self)
    }

    fn into_variant<V: VariantOf<Self>>(self) -> Option<V> {
        V::into_variant(self)
    }

    fn as_variant<V: VariantOf<Self>>(&self) -> Option<&V> {
        V::as_variant(self)
    }

    fn as_variant_mut<V: VariantOf<Self>>(&mut self) -> Option<&mut V> {
        V::as_variant_mut(self)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: There has to be a crate for this
pub trait VariantOf<E>
where
    Self: Sized,
{
    fn is_variant(e: &E) -> bool;
    fn into_variant(e: E) -> Option<Self>;
    fn as_variant(e: &E) -> Option<&Self>;
    fn as_variant_mut(e: &mut E) -> Option<&mut Self>;
}

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_event_variant {
    ($typ:ident) => {
        impl From<$typ> for MetadataEvent {
            fn from(v: $typ) -> MetadataEvent {
                MetadataEvent::$typ(v)
            }
        }

        impl VariantOf<MetadataEvent> for $typ {
            fn is_variant(e: &MetadataEvent) -> bool {
                match e {
                    MetadataEvent::$typ(_) => true,
                    _ => false,
                }
            }

            fn into_variant(e: MetadataEvent) -> Option<Self> {
                match e {
                    MetadataEvent::$typ(v) => Some(v),
                    _ => None,
                }
            }

            fn as_variant(e: &MetadataEvent) -> Option<&Self> {
                match e {
                    MetadataEvent::$typ(v) => Some(v),
                    _ => None,
                }
            }

            fn as_variant_mut(e: &mut MetadataEvent) -> Option<&mut Self> {
                match e {
                    MetadataEvent::$typ(v) => Some(v),
                    _ => None,
                }
            }
        }
    };
}

impl_event_variant!(AddData);
impl_event_variant!(ExecuteQuery);
impl_event_variant!(Seed);
impl_event_variant!(SetPollingSource);
impl_event_variant!(SetTransform);
impl_event_variant!(SetVocab);
impl_event_variant!(SetWatermark);

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataEventDataStream {
    pub output_data: Option<DataSlice>,
    pub output_watermark: Option<DateTime<Utc>>,
}

pub struct MetadataEventDataStreamRef<'a> {
    pub output_data: Option<&'a DataSlice>,
    pub output_watermark: Option<&'a DateTime<Utc>>,
}

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
        let (output_data, output_watermark) = match self.event {
            MetadataEvent::AddData(e) => (Some(e.output_data), e.output_watermark),
            MetadataEvent::ExecuteQuery(e) => (e.output_data, e.output_watermark),
            MetadataEvent::SetWatermark(e) => (None, Some(e.output_watermark)),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_) => return None,
        };
        Some(MetadataBlockDataStream {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash,
            event: MetadataEventDataStream {
                output_data,
                output_watermark,
            },
        })
    }

    fn as_data_stream_block<'a>(&'a self) -> Option<MetadataBlockDataStreamRef<'a>> {
        let (output_data, output_watermark) = match &self.event {
            MetadataEvent::AddData(e) => (Some(&e.output_data), e.output_watermark.as_ref()),
            MetadataEvent::ExecuteQuery(e) => (e.output_data.as_ref(), e.output_watermark.as_ref()),
            MetadataEvent::SetWatermark(e) => (None, Some(&e.output_watermark)),
            MetadataEvent::Seed(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_) => return None,
        };
        Some(MetadataBlockDataStreamRef {
            system_time: &self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            event: MetadataEventDataStreamRef {
                output_data,
                output_watermark,
            },
        })
    }
}
