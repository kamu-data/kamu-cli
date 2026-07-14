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
use crate::resource::ResourceSelector;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSelector(ResourceSelector);

impl std::ops::Deref for DatasetSelector {
    type Target = ResourceSelector;
    fn deref(&self) -> &ResourceSelector {
        &self.0
    }
}

impl std::str::FromStr for DatasetSelector {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(DatasetSelector);
impl_try_from_str!(DatasetSelector);

impl<'fb> FlatbuffersSerializable<'fb> for DatasetSelector {
    type OffsetT = WIPOffset<fb::DatasetSelector<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSelector<'fb>> for DatasetSelector {
    fn deserialize(_proxy: fb::DatasetSelector<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
