// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use super::{ResourceID, ResourceName, TypeName, TypeRef};
use crate::auth::{AccountName, AccountRef};
use crate::formats::*;
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceRef {
    Id {
        account: Option<AccountRef>,
        typ: TypeRef,
        id: ResourceID,
    },
    Name {
        account: Option<AccountRef>,
        typ: TypeRef,
        name: ResourceName,
    },
    Both {
        account: Option<AccountRef>,
        typ: TypeRef,
        id: ResourceID,
        name: ResourceName,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceRef {
    pub fn new(typ: TypeRef, name: ResourceName) -> Self {
        Self::Name {
            account: None,
            typ,
            name,
        }
    }

    pub fn account(&self) -> Option<&AccountRef> {
        match self {
            Self::Id { account, .. } | Self::Name { account, .. } | Self::Both { account, .. } => {
                account.as_ref()
            }
        }
    }

    pub fn typ(&self) -> &TypeRef {
        match self {
            Self::Id { typ, .. } | Self::Name { typ, .. } | Self::Both { typ, .. } => typ,
        }
    }

    pub fn id(&self) -> Option<&ResourceID> {
        match self {
            Self::Id { id, .. } | Self::Both { id, .. } => Some(id),
            Self::Name { .. } => None,
        }
    }

    pub fn name(&self) -> Option<&ResourceName> {
        match self {
            Self::Name { name, .. } | Self::Both { name, .. } => Some(name),
            Self::Id { .. } => None,
        }
    }
}

impl_parse_error!(ResourceRef);
impl_try_from_str!(ResourceRef);

impl std::str::FromStr for ResourceRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((typ, account, name)) = Grammar::match_resource_ref(s) else {
            return Err(::multiformats::ParseError::<Self>::new(s));
        };

        Ok(Self::Name {
            account: account.map(|s| AccountName::new_unchecked(s).into()),
            typ: TypeName::new_unchecked(typ).into(),
            name: ResourceName::new_unchecked(name),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for ResourceRef {
    type OffsetT = WIPOffset<fb::ResourceRef<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let account_offset = self.account().as_ref().map(|v| v.serialize(fb));
        let type_offset = fb.create_string(self.typ().as_str());
        let id_offset = self.id().as_ref().map(|v| fb.create_vector(v.as_bytes()));
        let name_offset = self.name().as_ref().map(|v| fb.create_string(v.as_ref()));
        let mut builder = fb::ResourceRefBuilder::new(fb);
        account_offset.map(|off| builder.add_account(off));
        builder.add_type_(type_offset);
        id_offset.map(|off| builder.add_id(off));
        name_offset.map(|off| builder.add_name(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceRef<'fb>> for ResourceRef {
    fn deserialize(proxy: fb::ResourceRef<'fb>) -> Self {
        let s = crate::serde::yaml::resource::ResourceRef {
            account: proxy
                .account()
                .map(|v| crate::auth::AccountRef::deserialize(v).into()),
            r#type: proxy.type_().unwrap().parse().unwrap(),
            id: proxy
                .id()
                .map(|v| ResourceID::from_bytes(v.bytes()).unwrap()),
            name: proxy.name().map(|v| ResourceName::try_from(v).unwrap()),
        };
        s.try_into().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
