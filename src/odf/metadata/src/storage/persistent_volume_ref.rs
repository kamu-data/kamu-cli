// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::formats::*;
use crate::resource::ResourceRef;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentVolumeRef(ResourceRef);

impl PersistentVolumeRef {
    pub fn new_unchecked(r: ResourceRef) -> Self {
        Self(r)
    }

    pub fn into_inner(self) -> ResourceRef {
        self.0
    }
}

impl std::ops::Deref for PersistentVolumeRef {
    type Target = ResourceRef;
    fn deref(&self) -> &ResourceRef {
        &self.0
    }
}

impl std::str::FromStr for PersistentVolumeRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(PersistentVolumeRef);
impl_try_from_str!(PersistentVolumeRef);

impl<'fb> FlatbuffersSerializable<'fb> for PersistentVolumeRef {
    type OffsetT = WIPOffset<fb::PersistentVolumeRef<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PersistentVolumeRef<'fb>> for PersistentVolumeRef {
    fn deserialize(_proxy: fb::PersistentVolumeRef<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
