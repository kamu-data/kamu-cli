// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::auth::*;
use crate::resources::ResourceRef;
use crate::serde::flatbuffers::{
    FlatbuffersEnumDeserializable,
    FlatbuffersEnumSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectHandle {
    Principal(PrincipalHandle),
    Group(ResourceRef),
    Authenticated,
    Anonymous,
    Everyone,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<PrincipalHandle> for SubjectHandle {
    fn from(value: PrincipalHandle) -> Self {
        Self::Principal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::SubjectHandle> for SubjectHandle {
    fn serialize(
        &self,
        _fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::SubjectHandle, WIPOffset<UnionWIPOffset>) {
        todo!()
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SubjectHandle> for SubjectHandle {
    fn deserialize(_table: flatbuffers::Table<'fb>, _t: fb::SubjectHandle) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
