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

pub struct MetadataBlockTyped<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub event: &'a T,
}

pub struct MetadataBlockTypedMut<'a, T> {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<&'a Multihash>,
    pub event: &'a mut T,
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Allows "casting" a generic MetadataBlock into one containing a specific event type
pub trait AsTypedBlock {
    fn as_typed<'a, T: VariantOf<MetadataEvent>>(&'a self) -> Option<MetadataBlockTyped<'a, T>>;
    fn as_typed_mut<'a, T: VariantOf<MetadataEvent>>(
        &'a mut self,
    ) -> Option<MetadataBlockTypedMut<'a, T>>;
}

impl AsTypedBlock for MetadataBlock {
    fn as_typed<'a, T: VariantOf<MetadataEvent>>(&'a self) -> Option<MetadataBlockTyped<'a, T>> {
        T::as_variant(&self.event).map(|e| MetadataBlockTyped {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            event: e,
        })
    }

    fn as_typed_mut<'a, T: VariantOf<MetadataEvent>>(
        &'a mut self,
    ) -> Option<MetadataBlockTypedMut<'a, T>> {
        T::as_variant_mut(&mut self.event).map(|e| MetadataBlockTypedMut {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash.as_ref(),
            event: e,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub trait EnumWithVariants<E> {
    fn as_variant<V: VariantOf<E>>(&self) -> Option<&V>;
    fn as_variant_mut<V: VariantOf<E>>(&mut self) -> Option<&mut V>;
}

impl EnumWithVariants<MetadataEvent> for MetadataEvent {
    fn as_variant<V: VariantOf<Self>>(&self) -> Option<&V> {
        V::as_variant(self)
    }

    fn as_variant_mut<V: VariantOf<Self>>(&mut self) -> Option<&mut V> {
        V::as_variant_mut(self)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: There has to be a crate for this
pub trait VariantOf<E> {
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
