// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceID
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ResourceID(uuid::Uuid);

impl ResourceID {
    pub fn new(uid: uuid::Uuid) -> Self {
        Self(uid)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, multiformats::DeserializeError<Self>> {
        Self::try_from(bytes)
    }
}

impl_parse_error!(ResourceID);
impl_try_from_str!(ResourceID);

impl std::str::FromStr for ResourceID {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(
            uuid::Uuid::parse_str(s).map_err(|e| Self::Err::new_from("invalid UUID", e))?,
        ))
    }
}

impl TryFrom<&[u8]> for ResourceID {
    type Error = multiformats::DeserializeError<Self>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self::new(uuid::Uuid::from_slice(value).map_err(
            multiformats::DeserializeError::<Self>::new_from,
        )?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for ResourceID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AsRef<uuid::Uuid> for ResourceID {
    fn as_ref(&self) -> &uuid::Uuid {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    ResourceName,
    Grammar::match_resource_name,
    ResourceNameSerdeVisitor
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
