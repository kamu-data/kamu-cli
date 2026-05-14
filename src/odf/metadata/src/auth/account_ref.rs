// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use super::*;
use crate::formats::*;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountRef {
    Id(AccountID),
    Name(AccountName),
    Both { id: AccountID, name: AccountName },
}

impl AccountRef {
    pub fn id(&self) -> Option<&AccountID> {
        match self {
            AccountRef::Id(id) | AccountRef::Both { id, .. } => Some(id),
            AccountRef::Name(_) => None,
        }
    }

    pub fn name(&self) -> Option<&AccountName> {
        match self {
            AccountRef::Name(name) | AccountRef::Both { name, .. } => Some(name),
            AccountRef::Id(_) => None,
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
        // let id_offset = self.id.as_ref().map(|v|
        // fb.create_vector(&v.as_bytes())); let name_offset =
        // self.name.as_ref().map(|v| fb.create_string(&v.to_string()));
        // let mut builder = fb::AccountRefBuilder::new(fb);
        // id_offset.map(|off| builder.add_id(off));
        // name_offset.map(|off| builder.add_name(off));
        // builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AccountRef<'fb>> for AccountRef {
    fn deserialize(_proxy: fb::AccountRef<'fb>) -> Self {
        todo!()
        // Self {
        //     id: proxy
        //         .id()
        //         .map(|v| AccountID::from_bytes(v.bytes()).unwrap()),
        //     name: proxy
        //         .name()
        //         .map(|v| super::AccountName::try_from(v).unwrap()),
        // }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
