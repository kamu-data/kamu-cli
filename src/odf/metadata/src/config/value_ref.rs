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

// TODO: Later move into `config` sub-module
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueRef {
    pub resource: ResourceRef,
    pub path: Option<String>,
}

impl ValueRef {
    pub fn new(resource: ResourceRef, path: Option<String>) -> Self {
        Self { resource, path }
    }
}

impl std::ops::Deref for ValueRef {
    type Target = ResourceRef;
    fn deref(&self) -> &ResourceRef {
        &self.resource
    }
}

impl std::str::FromStr for ValueRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(ValueRef);
impl_try_from_str!(ValueRef);

impl<'fb> FlatbuffersSerializable<'fb> for ValueRef {
    type OffsetT = WIPOffset<fb::ValueRef<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ValueRef<'fb>> for ValueRef {
    fn deserialize(_proxy: fb::ValueRef<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
