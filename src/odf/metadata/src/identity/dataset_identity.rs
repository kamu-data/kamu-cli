// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::convert::{AsRef, TryFrom};
use std::hash::Hash;
use std::sync::Arc;
use std::{cmp, fmt, ops};

use super::grammar::Grammar;
use super::{DatasetRef, DatasetRefAny, DatasetRefRemote};
use crate::DatasetHandle;
use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Macro helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_parse_error {
    ($typ:ident) => {
        impl ::multiformats::Multiformat for $typ {
            fn format_name() -> &'static str {
                stringify!($typ)
            }
        }
    };
}

pub(crate) use impl_parse_error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with AsRef matcher
// This is a workaround for: https://github.com/rust-lang/rust/issues/50133
macro_rules! impl_try_from_str {
    ($typ:ident) => {
        impl TryFrom<&str> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s)
            }
        }

        impl TryFrom<String> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl TryFrom<&String> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl TryFrom<&std::ffi::OsString> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &std::ffi::OsString) -> Result<Self, Self::Error> {
                // TODO: May not always be convertible
                <Self as std::str::FromStr>::from_str(s.to_str().unwrap())
            }
        }
    };
}

pub(crate) use impl_try_from_str;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_serde {
    ($typ:ident, $visitor:ident) => {
        impl serde::Serialize for $typ {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> serde::Deserialize<'de> for $typ {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                deserializer.deserialize_string($visitor)
            }
        }

        struct $visitor;

        impl<'de> serde::de::Visitor<'de> for $visitor {
            type Value = $typ;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a {} string", stringify!($typ))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                $typ::try_from(v).map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use impl_serde;
use like::ILike;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! newtype_str {
    ($typ:ident, $parse:expr, $visitor:ident) => {
        #[derive(Debug, Clone, Eq)]
        pub struct $typ(Arc<str>);

        impl $typ {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> Self {
                Self(Arc::from(s.as_ref()))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }

            pub fn into_inner(self) -> Arc<str> {
                self.0
            }

            pub fn from_inner_unchecked(s: Arc<str>) -> Self {
                Self(s)
            }

            pub fn into_lowercase(s: &str) -> Cow<'_, str> {
                let bytes = s.as_bytes();
                if !bytes.iter().any(u8::is_ascii_uppercase) {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(s.to_ascii_lowercase())
                }
            }
        }

        impl From<$typ> for String {
            fn from(v: $typ) -> String {
                (*v.0).into()
            }
        }

        impl From<&$typ> for String {
            fn from(v: &$typ) -> String {
                (*v.0).into()
            }
        }

        impl From<&$typ> for $typ {
            fn from(v: &$typ) -> Self {
                v.clone()
            }
        }

        impl std::str::FromStr for $typ {
            type Err = ::multiformats::ParseError<$typ>;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match $parse(s) {
                    Some((_, "")) => Ok(Self::new_unchecked(s)),
                    _ => Err(ParseError::new(s)),
                }
            }
        }

        impl PartialEq for $typ {
            fn eq(&self, other: &$typ) -> bool {
                self.eq_ignore_ascii_case(other)
            }
        }

        impl Hash for $typ {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                Self::into_lowercase(&self.0).hash(state);
            }
        }

        impl PartialOrd for $typ {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $typ {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                Self::into_lowercase(self).cmp(&Self::into_lowercase(other))
            }
        }

        impl ops::Deref for $typ {
            type Target = str;

            fn deref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<str> for $typ {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<std::path::Path> for $typ {
            fn as_ref(&self) -> &std::path::Path {
                (*self.0).as_ref()
            }
        }

        impl cmp::PartialEq<&str> for $typ {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl cmp::PartialEq<&str> for &$typ {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl fmt::Display for $typ {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        impl_try_from_str!($typ);

        impl_serde!($typ, $visitor);

        impl_parse_error!($typ);
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    DatasetName,
    Grammar::match_dataset_name,
    DatasetNameSerdeVisitor
);

impl DatasetName {
    pub fn as_local_ref(&self) -> DatasetRef {
        DatasetRef::Alias(DatasetAlias::new(None, self.clone()))
    }

    pub fn into_local_ref(self) -> DatasetRef {
        DatasetRef::Alias(DatasetAlias::new(None, self))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    DatasetNamePattern,
    Grammar::match_dataset_name_pattern,
    DatasetNamePatternSerdeVisitor
);

impl DatasetNamePattern {
    pub fn matches(&self, dataset_name: &DatasetName) -> bool {
        ILike::<false>::ilike(dataset_name.as_str(), self).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetAliasPattern {
    pub account_name: Option<AccountName>,
    pub dataset_name_pattern: DatasetNamePattern,
}

impl DatasetAliasPattern {
    pub fn new(
        account_name: Option<AccountName>,
        dataset_name_pattern: DatasetNamePattern,
    ) -> Self {
        Self {
            account_name,
            dataset_name_pattern,
        }
    }

    pub fn matches(&self, dataset_handle: &DatasetHandle) -> bool {
        (self.account_name.is_none() || self.account_name == dataset_handle.alias.account_name)
            && self
                .dataset_name_pattern
                .matches(&dataset_handle.alias.dataset_name)
    }
}

impl std::str::FromStr for DatasetAliasPattern {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('/') {
            Some((account, dataset_name)) => match DatasetNamePattern::try_from(dataset_name) {
                Ok(dataset_name_pattern) => match AccountName::try_from(account) {
                    Ok(account_name) => Ok(Self {
                        account_name: Some(account_name),
                        dataset_name_pattern,
                    }),
                    Err(_) => Err(Self::Err::new(s)),
                },
                Err(_) => Err(Self::Err::new(s)),
            },
            None => match DatasetNamePattern::try_from(s) {
                Ok(dataset_name_pattern) => Ok(Self {
                    account_name: None,
                    dataset_name_pattern,
                }),
                Err(_) => Err(Self::Err::new(s)),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    AccountName,
    Grammar::match_account_name,
    AccountNameSerdeVisitor
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

newtype_str!(RepoName, Grammar::match_repo_name, RepoNameSerdeVisitor);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasetAlias {
    pub account_name: Option<AccountName>,
    pub dataset_name: DatasetName,
}

impl DatasetAlias {
    pub fn new(account_name: Option<AccountName>, dataset_name: DatasetName) -> Self {
        Self {
            account_name,
            dataset_name,
        }
    }

    pub fn is_multi_tenant(&self) -> bool {
        self.account_name.is_some()
    }

    pub fn as_local_ref(&self) -> DatasetRef {
        DatasetRef::Alias(self.clone())
    }

    pub fn into_local_ref(self) -> DatasetRef {
        DatasetRef::Alias(self)
    }

    pub fn as_remote_alias(&self, repo_name: impl Into<RepoName>) -> DatasetAliasRemote {
        DatasetAliasRemote::new(
            repo_name.into(),
            self.account_name.clone(),
            self.dataset_name.clone(),
        )
    }

    pub fn into_remote_alias(self, repo_name: impl Into<RepoName>) -> DatasetAliasRemote {
        DatasetAliasRemote::new(repo_name.into(), self.account_name, self.dataset_name)
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

impl std::str::FromStr for DatasetAlias {
    type Err = ParseError<Self>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_dataset_alias(s) {
            Some((acc, ds, "")) => Ok(Self::new(
                acc.map(AccountName::new_unchecked),
                DatasetName::new_unchecked(ds),
            )),
            _ => Err(ParseError::new(s)),
        }
    }
}

impl fmt::Display for DatasetAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(acc) = &self.account_name {
            write!(f, "{acc}/")?;
        }
        write!(f, "{}", self.dataset_name)
    }
}

impl_try_from_str!(DatasetAlias);

impl_parse_error!(DatasetAlias);

impl_serde!(DatasetAlias, DatasetAliasSerdeVisitor);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasetAliasRemote {
    pub repo_name: RepoName,
    pub account_name: Option<AccountName>,
    pub dataset_name: DatasetName,
}

impl DatasetAliasRemote {
    pub fn new(
        repo_name: RepoName,
        account_name: Option<AccountName>,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repo_name,
            account_name,
            dataset_name,
        }
    }

    pub fn is_multi_tenant(&self) -> bool {
        self.account_name.is_some()
    }

    pub fn local_alias(&self) -> DatasetAlias {
        DatasetAlias::new(self.account_name.clone(), self.dataset_name.clone())
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::Alias(self.clone())
    }

    pub fn into_remote_ref(self) -> DatasetRefRemote {
        DatasetRefRemote::Alias(self)
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::RemoteAlias(
            self.repo_name.clone(),
            self.account_name.clone(),
            self.dataset_name.clone(),
        )
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::RemoteAlias(self.repo_name, self.account_name, self.dataset_name)
    }
}

impl std::str::FromStr for DatasetAliasRemote {
    type Err = ParseError<Self>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_dataset_alias_remote(s) {
            Some((repo, acc, ds, "")) => Ok(Self::new(
                RepoName::new_unchecked(repo),
                acc.map(AccountName::new_unchecked),
                DatasetName::new_unchecked(ds),
            )),
            _ => Err(ParseError::new(s)),
        }
    }
}

impl fmt::Display for DatasetAliasRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.repo_name)?;
        if let Some(acc) = &self.account_name {
            write!(f, "{acc}/")?;
        }
        write!(f, "{}", self.dataset_name)
    }
}

impl_try_from_str!(DatasetAliasRemote);

impl_parse_error!(DatasetAliasRemote);
impl_parse_error!(DatasetAliasPattern);

impl_serde!(DatasetAliasRemote, DatasetAliasRemoteSerdeVisitor);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for DatasetName {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for DatasetName {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!("my-dataset")])
                .build(),
        )
        .into()
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for AccountName {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for AccountName {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!("my-account")])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
