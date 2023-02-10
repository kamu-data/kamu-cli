// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::convert::{AsRef, TryFrom};
use std::fmt;
use std::ops;
use std::sync::Arc;

use ed25519_dalek::Keypair;

use super::grammar::Grammar;
use super::{DatasetRefAny, DatasetRefLocal, DatasetRefRemote};
use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////
// Macro helpers
////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with AsRef matcher
// This is a workaround for: https://github.com/rust-lang/rust/issues/50133
macro_rules! impl_try_from_str {
    ($typ:ident) => {
        impl TryFrom<&str> for $typ {
            type Error = InvalidValue<$typ>;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s)
            }
        }

        impl TryFrom<String> for $typ {
            type Error = InvalidValue<$typ>;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl TryFrom<&String> for $typ {
            type Error = InvalidValue<$typ>;
            fn try_from(s: &String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl TryFrom<&std::ffi::OsString> for $typ {
            type Error = InvalidValue<$typ>;
            fn try_from(s: &std::ffi::OsString) -> Result<Self, Self::Error> {
                // TODO: May not always be convertible
                <Self as std::str::FromStr>::from_str(s.to_str().unwrap())
            }
        }
    };
}

pub(crate) use impl_try_from_str;

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

macro_rules! newtype_str {
    ($typ:ident, $parse:expr, $visitor:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $typ(Arc<str>);

        impl $typ {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> Self {
                Self(Arc::from(s.as_ref()))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl From<&$typ> for $typ {
            fn from(v: &$typ) -> Self {
                v.clone()
            }
        }

        impl Into<String> for $typ {
            fn into(self) -> String {
                (*self.0).into()
            }
        }

        impl Into<String> for &$typ {
            fn into(self) -> String {
                (*self.0).into()
            }
        }

        impl std::str::FromStr for $typ {
            type Err = InvalidValue<$typ>;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match $parse(s) {
                    Some((_, "")) => Ok(Self::new_unchecked(s)),
                    _ => Err(InvalidValue::new(s)),
                }
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

        impl_invalid_value!($typ);
    };
}

////////////////////////////////////////////////////////////////////////////////
// DatasetID
////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the dataset
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetID {
    pub cid: CID,
}

impl DatasetID {
    pub fn new(cid: CID) -> Self {
        assert_eq!(cid.content_type(), Multicodec::Ed25519Pub);
        Self { cid }
    }

    pub fn from_new_keypair_ed25519() -> (Keypair, Self) {
        use rand::rngs::OsRng;

        let mut csprng = OsRng {};
        let keypair: Keypair = Keypair::generate(&mut csprng);
        let pub_key = keypair.public.to_bytes();
        let id = Self::from_pub_key_ed25519(&pub_key);
        (keypair, id)
    }

    pub fn from_pub_key_ed25519(key: &[u8]) -> Self {
        Self::new(CID::new(
            Multicodec::Ed25519Pub,
            Multihash::from_digest_sha3_256(key),
        ))
    }

    // TODO: PERF: Performance
    pub fn to_bytes(&self) -> Vec<u8> {
        self.cid.to_bytes()
    }

    // TODO: PERF: Performance
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CIDError> {
        CID::from_bytes(bytes).map(Self::new)
    }

    // TODO: PERF: Performance
    pub fn to_did_string(&self) -> String {
        format!("did:odf:{}", self.cid.to_multibase_string())
    }

    // TODO: PERF: Performance
    pub fn from_did_string(s: &str) -> Result<Self, InvalidValue<DatasetID>> {
        if !s.starts_with("did:odf:") {
            return Err(InvalidValue::new(s));
        }
        CID::from_multibase_str(&s[8..])
            .map(Self::new)
            .map_err(|_| InvalidValue::new(s))
    }

    pub fn as_local_ref(&self) -> DatasetRefLocal {
        DatasetRefLocal::ID(self.clone())
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::ID(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::ID(self.clone())
    }
}

impl std::str::FromStr for DatasetID {
    type Err = InvalidValue<DatasetID>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DatasetID::from_did_string(s)
    }
}

impl_try_from_str!(DatasetID);

impl_serde!(DatasetID, DatasetIDSerdeVisitor);

impl_invalid_value!(DatasetID);

impl fmt::Debug for DatasetID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DatasetID")
            .field(&self.to_did_string())
            .finish()
    }
}

impl fmt::Display for DatasetID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_did_string())
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    DatasetName,
    Grammar::match_dataset_name,
    DatasetNameSerdeVisitor
);

impl DatasetName {
    pub fn as_local_ref(&self) -> DatasetRefLocal {
        DatasetRefLocal::Name(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::Name(self.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    AccountName,
    Grammar::match_account_name,
    AccountNameSerdeVisitor
);

////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    RepositoryName,
    Grammar::match_repository_name,
    RepositoryNameSerdeVisitor
);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasetNameWithOwner {
    account_name: Option<AccountName>,
    dataset_name: DatasetName,
}

impl DatasetNameWithOwner {
    pub fn new(account_name: Option<AccountName>, dataset_name: DatasetName) -> Self {
        Self {
            account_name,
            dataset_name,
        }
    }

    pub fn is_multitenant(&self) -> bool {
        self.account_name.is_some()
    }

    pub fn dataset(&self) -> &DatasetName {
        &self.dataset_name
    }

    pub fn account(&self) -> Option<&AccountName> {
        self.account_name.as_ref()
    }

    pub fn as_remote_name(&self, repository_name: &RepositoryName) -> RemoteDatasetName {
        RemoteDatasetName::new(
            repository_name.clone(),
            self.account_name.clone(),
            self.dataset_name.clone(),
        )
    }
}

impl std::str::FromStr for DatasetNameWithOwner {
    type Err = InvalidValue<DatasetNameWithOwner>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_dataset_name_with_owner(s) {
            Some((acc, ds, "")) => Ok(Self::new(
                acc.map(|s| AccountName::new_unchecked(s)),
                DatasetName::new_unchecked(ds),
            )),
            _ => Err(InvalidValue::new(s)),
        }
    }
}

impl fmt::Display for DatasetNameWithOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(acc) = &self.account_name {
            write!(f, "{}/", acc)?;
        }
        write!(f, "{}", self.dataset_name)
    }
}

impl_try_from_str!(DatasetNameWithOwner);

impl_invalid_value!(DatasetNameWithOwner);

impl_serde!(DatasetNameWithOwner, DatasetNameWithOwnerSerdeVisitor);

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemoteDatasetName {
    repository_name: RepositoryName,
    account_name: Option<AccountName>,
    dataset_name: DatasetName,
}

impl RemoteDatasetName {
    pub fn new(
        repository_name: RepositoryName,
        account_name: Option<AccountName>,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repository_name,
            account_name,
            dataset_name,
        }
    }

    pub fn is_multitenant(&self) -> bool {
        self.account_name.is_some()
    }

    pub fn dataset(&self) -> &DatasetName {
        &self.dataset_name
    }

    pub fn account(&self) -> Option<&AccountName> {
        self.account_name.as_ref()
    }

    pub fn repository(&self) -> &RepositoryName {
        &self.repository_name
    }

    pub fn as_name_with_owner(&self) -> DatasetNameWithOwner {
        DatasetNameWithOwner::new(
            self.account_name.as_ref().map(|a| a.clone()), 
            self.dataset_name.clone()
        )
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::RemoteName(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::RemoteName(self.clone())
    }
}

impl std::str::FromStr for RemoteDatasetName {
    type Err = InvalidValue<RemoteDatasetName>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_remote_dataset_name(s) {
            Some((repo, acc, ds, "")) => Ok(Self::new(
                RepositoryName::new_unchecked(repo),
                acc.map(|s| AccountName::new_unchecked(s)),
                DatasetName::new_unchecked(ds),
            )),
            _ => Err(InvalidValue::new(s)),
        }
    }
}

impl fmt::Display for RemoteDatasetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.repository_name)?;
        if let Some(acc) = &self.account_name {
            write!(f, "{}/", acc)?;
        }
        write!(f, "{}", self.dataset_name)
    }
}

impl_try_from_str!(RemoteDatasetName);

impl_invalid_value!(RemoteDatasetName);

impl_serde!(RemoteDatasetName, RemoteDatasetNameSerdeVisitor);
