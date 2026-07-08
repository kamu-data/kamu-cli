// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde::{Deserialize, Serialize};

use super::*;
use crate::formats::*;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AccountRefProxy = crate::serde::yaml::StructOrString<crate::serde::yaml::auth::AccountRef>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AccountRefProxy", into = "AccountRefProxy")]
pub enum AccountRef {
    Id(AccountID),
    Name(AccountName),
    Handle(AccountHandle),
}

impl AccountRef {
    pub fn id(&self) -> Option<&AccountID> {
        match self {
            AccountRef::Id(id) => Some(id),
            AccountRef::Name(_) => None,
            AccountRef::Handle(hdl) => Some(&hdl.id),
        }
    }

    pub fn name(&self) -> Option<&AccountName> {
        match self {
            AccountRef::Name(name) => Some(name),
            AccountRef::Id(_) => None,
            AccountRef::Handle(hdl) => Some(&hdl.name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<AccountID> for AccountRef {
    fn from(value: AccountID) -> Self {
        Self::Id(value)
    }
}

impl From<AccountName> for AccountRef {
    fn from(value: AccountName) -> Self {
        Self::Name(value)
    }
}

impl From<AccountHandle> for AccountRef {
    fn from(value: AccountHandle) -> Self {
        Self::Handle(value)
    }
}

impl From<AccountRef> for crate::resource::ResourceRef {
    fn from(_v: AccountRef) -> Self {
        todo!(
            "We need a way to use DIDs in ResourceRef to implement this. ResourceRef likely \
             should have some ResourceIDRef type that allows both UUIDs and DIDs"
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for AccountRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::Name(
            s.parse().map_err(::multiformats::ParseError::convert)?,
        ))
    }
}

impl_parse_error!(AccountRef);
impl_try_from_str!(AccountRef);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for AccountRef {
    type OffsetT = WIPOffset<fb::AccountRef<'fb>>;

    fn serialize(&self, _fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        todo!()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AccountRef<'fb>> for AccountRef {
    fn deserialize(_proxy: fb::AccountRef<'fb>) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
