// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use multiformats::ParseError;

use crate::auth::*;
use crate::resources::ResourceRef;
use crate::serde::flatbuffers::{
    FlatbuffersEnumDeserializable,
    FlatbuffersEnumSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectRef {
    Principal(PrincipalRef),
    Group(ResourceRef),
    Authenticated,
    Anonymous,
    Everyone,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<PrincipalRef> for SubjectRef {
    fn from(value: PrincipalRef) -> Self {
        Self::Principal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

crate::impl_parse_error!(SubjectRef);

crate::impl_try_from_str!(SubjectRef);

impl std::str::FromStr for SubjectRef {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (typ, rest) = s.split_once(':').ok_or_else(|| ParseError::new(s))?;

        match (typ, rest) {
            ("Authenticated" | "authenticated", "*") => Ok(Self::Authenticated),
            ("Anonymous" | "anonymous", "*") => Ok(Self::Anonymous),
            ("Everyone" | "everyone", "*") => Ok(Self::Everyone),
            ("Group" | "group", _) => {
                let res = ResourceRef::from_str(s).map_err(ParseError::convert)?;
                assert!(res.did.is_none());
                Ok(Self::Group(res))
            }
            _ => Ok(PrincipalRef::from_str(s)
                .map_err(ParseError::convert)?
                .into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::SubjectRef> for SubjectRef {
    fn serialize(
        &self,
        _fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::SubjectRef, WIPOffset<UnionWIPOffset>) {
        todo!()
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SubjectRef> for SubjectRef {
    fn deserialize(_table: flatbuffers::Table<'fb>, _t: fb::SubjectRef) -> Self {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
