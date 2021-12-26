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
use std::marker::PhantomData;
use std::ops;
use std::sync::Arc;

use ed25519_dalek::Keypair;

use super::grammar::Grammar;
use super::{CIDError, CID};
use super::{DatasetRefAny, DatasetRefLocal, DatasetRefRemote};
use super::{Multicodec, Multihash};

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

macro_rules! impl_invalid_value {
    ($typ:ident) => {
        impl fmt::Debug for InvalidValue<$typ> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "InvalidValue<{}>({:?})", stringify!($typ), self.value)
            }
        }

        impl fmt::Display for InvalidValue<$typ> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "Invalid {}: {}", stringify!($typ), self.value)
            }
        }

        impl std::error::Error for InvalidValue<$typ> {}
    };
}

pub(crate) use impl_invalid_value;

macro_rules! newtype_str {
    ($buf_type:ident, $parse:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $buf_type(Arc<str>);

        impl $buf_type {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> Self {
                Self(Arc::from(s.as_ref()))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl From<&$buf_type> for $buf_type {
            fn from(v: &$buf_type) -> Self {
                v.clone()
            }
        }

        impl Into<String> for $buf_type {
            fn into(self) -> String {
                (*self.0).into()
            }
        }

        impl Into<String> for &$buf_type {
            fn into(self) -> String {
                (*self.0).into()
            }
        }

        impl std::str::FromStr for $buf_type {
            type Err = InvalidValue<$buf_type>;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match $parse(s) {
                    Some((_, "")) => Ok(Self::new_unchecked(s)),
                    _ => Err(InvalidValue::new(s)),
                }
            }
        }

        impl_try_from_str!($buf_type);

        impl ops::Deref for $buf_type {
            type Target = str;

            fn deref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<str> for $buf_type {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<std::path::Path> for $buf_type {
            fn as_ref(&self) -> &std::path::Path {
                (*self.0).as_ref()
            }
        }

        impl cmp::PartialEq<&str> for $buf_type {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl fmt::Display for $buf_type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        impl_invalid_value!($buf_type);
    };
}

////////////////////////////////////////////////////////////////////////////////
// DatasetID
////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the dataset
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetID {
    cid: CID,
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

impl serde::Serialize for DatasetID {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_did_string())
    }
}

impl<'de> serde::Deserialize<'de> for DatasetID {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(DatasetIDSerdeVisitor)
    }
}

struct DatasetIDSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for DatasetIDSerdeVisitor {
    type Value = DatasetID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a DatasetID string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        DatasetID::from_did_string(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(DatasetName, Grammar::match_dataset_name);

impl DatasetName {
    pub fn as_local_ref(&self) -> DatasetRefLocal {
        DatasetRefLocal::Name(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::Name(self.clone())
    }
}

impl serde::Serialize for DatasetName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for DatasetName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(DatasetNameSerdeVisitor)
    }
}

struct DatasetNameSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for DatasetNameSerdeVisitor {
    type Value = DatasetName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a DatasetName string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        DatasetName::try_from(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(AccountName, Grammar::match_account_name);

////////////////////////////////////////////////////////////////////////////////

newtype_str!(RepositoryName, Grammar::match_repository_name);

////////////////////////////////////////////////////////////////////////////////

newtype_str!(RemoteDatasetName, Grammar::match_remote_dataset_name);

impl RemoteDatasetName {
    pub fn new(
        repo: &RepositoryName,
        account: Option<&AccountName>,
        dataset: &DatasetName,
    ) -> Self {
        let mut s = String::new();
        s.push_str(repo);
        s.push('/');
        if let Some(acc) = account {
            s.push_str(acc);
            s.push('/');
        }
        s.push_str(dataset);
        Self::new_unchecked(&s)
    }

    pub fn is_multitenant(&self) -> bool {
        self.0.chars().filter(|c| *c == '/').count() == 2
    }

    pub fn dataset(&self) -> DatasetName {
        DatasetName::new_unchecked(self.0.rsplit('/').next().unwrap())
    }

    pub fn account(&self) -> Option<AccountName> {
        let mut split = self.0.rsplit('/');
        split.next();
        let acc_or_repo = split.next();
        let maybe_repo = split.next();
        if maybe_repo.is_some() {
            acc_or_repo.map(|s| AccountName::new_unchecked(s))
        } else {
            None
        }
    }

    pub fn repository(&self) -> RepositoryName {
        let mut split = self.0.rsplit('/');
        split.next();
        let acc_or_repo = split.next();
        if let Some(repo) = split.next() {
            RepositoryName::new_unchecked(repo)
        } else {
            RepositoryName::new_unchecked(acc_or_repo.unwrap())
        }
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::RemoteName(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::RemoteName(self.clone())
    }
}

impl serde::Serialize for RemoteDatasetName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for RemoteDatasetName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(RemoteDatasetNameSerdeVisitor)
    }
}

struct RemoteDatasetNameSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for RemoteDatasetNameSerdeVisitor {
    type Value = RemoteDatasetName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a RemoteDatasetName string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        RemoteDatasetName::try_from(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct InvalidValue<T: ?Sized> {
    pub value: String,
    _ph: PhantomData<T>,
}

impl<T: ?Sized> InvalidValue<T> {
    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> Self {
        Self {
            value: s.as_ref().to_owned(),
            _ph: PhantomData,
        }
    }
}

impl fmt::Debug for InvalidValue<DatasetID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidValue<DatasetID>({:?})", self.value)
    }
}

impl fmt::Display for InvalidValue<DatasetID> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid DatasetID: {}", self.value)
    }
}
