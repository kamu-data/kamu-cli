// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::auth::AccountRef;
use crate::formats::*;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSelector {
    pub(crate) account: Option<AccountRef>,
}

impl ResourceSelector {
    pub fn new(account: Option<AccountRef>) -> Self {
        Self { account }
    }

    pub fn account(&self) -> Option<&AccountRef> {
        self.account.as_ref()
    }
}

impl std::str::FromStr for ResourceSelector {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl_parse_error!(ResourceSelector);
impl_try_from_str!(ResourceSelector);

impl<'fb> FlatbuffersSerializable<'fb> for ResourceSelector {
    type OffsetT = WIPOffset<fb::ResourceSelector<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceSelector<'fb>> for ResourceSelector {
    fn deserialize(_proxy: fb::ResourceSelector<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
