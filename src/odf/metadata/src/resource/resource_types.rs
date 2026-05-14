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
// TypeName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    TypeName,
    Grammar::match_resource_type_name,
    TypeNameSerdeVisitor
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TypeUri
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TypeUri(String);

impl TypeUri {
    pub fn new_unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for TypeUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TypeUri {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl_parse_error!(TypeUri);
impl_try_from_str!(TypeUri);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TypeRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeRef {
    Uri(TypeUri),
    Name(TypeName),
}

impl TypeRef {
    pub fn as_str(&self) -> &str {
        match self {
            TypeRef::Uri(v) => v.as_str(),
            TypeRef::Name(v) => v.as_str(),
        }
    }
}

impl From<TypeUri> for TypeRef {
    fn from(value: TypeUri) -> Self {
        Self::Uri(value)
    }
}

impl From<TypeName> for TypeRef {
    fn from(value: TypeName) -> Self {
        Self::Name(value)
    }
}

impl std::fmt::Display for TypeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for TypeRef {
    type Err = ::multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO: More sane impl
        if s.starts_with("https:") {
            Ok(Self::Uri(
                s.parse().map_err(::multiformats::ParseError::convert)?,
            ))
        } else {
            Ok(Self::Name(
                s.parse().map_err(::multiformats::ParseError::convert)?,
            ))
        }
    }
}

impl_parse_error!(TypeRef);
impl_try_from_str!(TypeRef);

impl serde::Serialize for TypeRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for TypeRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(TypeRefSerdeVisitor)
    }
}

struct TypeRefSerdeVisitor;

impl serde::de::Visitor<'_> for TypeRefSerdeVisitor {
    type Value = TypeRef;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a {} string", stringify!(TypeRef))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        TypeRef::try_from(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
