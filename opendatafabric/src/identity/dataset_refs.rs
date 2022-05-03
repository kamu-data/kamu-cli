// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::Arc;
use url::Url;

use super::dataset_handles::*;
use super::dataset_identity::*;
use super::grammar::Grammar;
use crate::formats::InvalidValue;

////////////////////////////////////////////////////////////////////////////////

/// References local dataset by ID or by name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRefLocal {
    ID(DatasetID),
    Name(DatasetName),
    Handle(DatasetHandle),
}

////////////////////////////////////////////////////////////////////////////////

/// References remote dataset by ID or by qualified name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRefRemote {
    ID(DatasetID),
    RemoteName(RemoteDatasetName),
    RemoteHandle(RemoteDatasetHandle),
    Url(Arc<Url>),
}

////////////////////////////////////////////////////////////////////////////////

/// References any dataset, local or remote
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRefAny {
    ID(DatasetID),
    Name(DatasetName),
    Handle(DatasetHandle),
    RemoteName(RemoteDatasetName),
    RemoteHandle(RemoteDatasetHandle),
    Url(Arc<Url>),
}

////////////////////////////////////////////////////////////////////////////////

/// References a dataset located inside a repository
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoDatasetRef {
    ID(DatasetID),
    Name(DatasetNameWithOwner),
    Url(Arc<Url>),
    Handle(RepoDatasetHandle),
}

////////////////////////////////////////////////////////////////////////////////
// DatasetRefLocal
////////////////////////////////////////////////////////////////////////////////

impl DatasetRefLocal {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            DatasetRefLocal::ID(id) => Some(id),
            DatasetRefLocal::Name(_) => None,
            DatasetRefLocal::Handle(DatasetHandle { id, .. }) => Some(id),
        }
    }

    pub fn name(&self) -> Option<&DatasetName> {
        match self {
            DatasetRefLocal::ID(_) => None,
            DatasetRefLocal::Name(name) => Some(name),
            DatasetRefLocal::Handle(DatasetHandle { name, .. }) => Some(name),
        }
    }
}

impl std::str::FromStr for DatasetRefLocal {
    type Err = InvalidValue<DatasetRefLocal>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetID::from_str(s) {
            Ok(id) => Ok(id.into()),
            Err(_) => match DatasetName::from_str(s) {
                Ok(name) => Ok(name.into()),
                Err(_) => Err(Self::Err::new(s)),
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(DatasetRefLocal);

crate::formats::impl_invalid_value!(DatasetRefLocal);

impl fmt::Display for DatasetRefLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatasetRefLocal::ID(v) => write!(f, "{}", v),
            DatasetRefLocal::Name(v) => write!(f, "{}", v),
            DatasetRefLocal::Handle(v) => write!(f, "{}", v),
        }
    }
}

impl From<DatasetID> for DatasetRefLocal {
    fn from(v: DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<&DatasetID> for DatasetRefLocal {
    fn from(v: &DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<DatasetName> for DatasetRefLocal {
    fn from(v: DatasetName) -> Self {
        Self::Name(v)
    }
}

impl From<&DatasetName> for DatasetRefLocal {
    fn from(v: &DatasetName) -> Self {
        Self::Name(v.clone())
    }
}

impl From<DatasetHandle> for DatasetRefLocal {
    fn from(v: DatasetHandle) -> Self {
        Self::Handle(v)
    }
}

impl From<&DatasetHandle> for DatasetRefLocal {
    fn from(v: &DatasetHandle) -> Self {
        Self::Handle(v.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetRefRemote
////////////////////////////////////////////////////////////////////////////////

impl DatasetRefRemote {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            DatasetRefRemote::ID(id) => Some(id),
            DatasetRefRemote::RemoteName(_) => None,
            DatasetRefRemote::RemoteHandle(RemoteDatasetHandle { id, .. }) => Some(id),
            DatasetRefRemote::Url(_) => None,
        }
    }

    pub fn name(&self) -> Option<&RemoteDatasetName> {
        match self {
            DatasetRefRemote::ID(_) => None,
            DatasetRefRemote::RemoteName(name) => Some(name),
            DatasetRefRemote::RemoteHandle(RemoteDatasetHandle { name, .. }) => Some(name),
            DatasetRefRemote::Url(_) => None,
        }
    }
}

impl std::str::FromStr for DatasetRefRemote {
    type Err = InvalidValue<DatasetRefRemote>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetID::from_str(s) {
            Ok(id) => Ok(id.into()),
            Err(_) => match RemoteDatasetName::from_str(s) {
                Ok(name) => Ok(name.into()),
                Err(_) => match Grammar::match_url(s) {
                    Some(_) => match Url::from_str(s) {
                        Ok(url) => Ok(url.into()),
                        Err(_) => Err(Self::Err::new(s)),
                    },
                    None => Err(Self::Err::new(s)),
                },
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(DatasetRefRemote);

crate::formats::impl_invalid_value!(DatasetRefRemote);

impl fmt::Display for DatasetRefRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatasetRefRemote::ID(v) => write!(f, "{}", v),
            DatasetRefRemote::RemoteName(v) => write!(f, "{}", v),
            DatasetRefRemote::RemoteHandle(v) => write!(f, "{}", v),
            DatasetRefRemote::Url(v) => write!(f, "{}", v),
        }
    }
}

impl From<DatasetID> for DatasetRefRemote {
    fn from(v: DatasetID) -> Self {
        Self::ID(v)
    }
}

impl From<&DatasetID> for DatasetRefRemote {
    fn from(v: &DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<RemoteDatasetName> for DatasetRefRemote {
    fn from(v: RemoteDatasetName) -> Self {
        Self::RemoteName(v)
    }
}

impl From<&RemoteDatasetName> for DatasetRefRemote {
    fn from(v: &RemoteDatasetName) -> Self {
        Self::RemoteName(v.clone())
    }
}

impl From<RemoteDatasetHandle> for DatasetRefRemote {
    fn from(v: RemoteDatasetHandle) -> Self {
        Self::RemoteHandle(v)
    }
}

impl From<&RemoteDatasetHandle> for DatasetRefRemote {
    fn from(v: &RemoteDatasetHandle) -> Self {
        Self::RemoteHandle(v.clone())
    }
}

impl From<Url> for DatasetRefRemote {
    fn from(v: Url) -> Self {
        Self::Url(Arc::new(v))
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetRefAny
////////////////////////////////////////////////////////////////////////////////

impl DatasetRefAny {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            DatasetRefAny::ID(id) => Some(id),
            DatasetRefAny::Name(_) => None,
            DatasetRefAny::Handle(DatasetHandle { id, .. }) => Some(id),
            DatasetRefAny::RemoteName(_) => None,
            DatasetRefAny::RemoteHandle(RemoteDatasetHandle { id, .. }) => Some(id),
            DatasetRefAny::Url(_) => None,
        }
    }

    pub fn as_local_ref(&self) -> Option<DatasetRefLocal> {
        match self {
            DatasetRefAny::ID(v) => Some(DatasetRefLocal::ID(v.clone())),
            DatasetRefAny::Name(v) => Some(DatasetRefLocal::Name(v.clone())),
            DatasetRefAny::Handle(v) => Some(DatasetRefLocal::Handle(v.clone())),
            DatasetRefAny::RemoteName(_) => None,
            DatasetRefAny::RemoteHandle(_) => None,
            DatasetRefAny::Url(_) => None,
        }
    }

    pub fn as_remote_ref(&self) -> Option<DatasetRefRemote> {
        match self {
            DatasetRefAny::ID(v) => Some(DatasetRefRemote::ID(v.clone())),
            DatasetRefAny::Name(_) => None,
            DatasetRefAny::Handle(_) => None,
            DatasetRefAny::RemoteName(v) => Some(DatasetRefRemote::RemoteName(v.clone())),
            DatasetRefAny::RemoteHandle(v) => Some(DatasetRefRemote::RemoteHandle(v.clone())),
            DatasetRefAny::Url(v) => Some(DatasetRefRemote::Url(v.clone())),
        }
    }
}

impl std::str::FromStr for DatasetRefAny {
    type Err = InvalidValue<DatasetRefAny>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetID::from_str(s) {
            Ok(id) => Ok(id.into()),
            Err(_) => match DatasetName::from_str(s) {
                Ok(name) => Ok(name.into()),
                Err(_) => match RemoteDatasetName::from_str(s) {
                    Ok(name) => Ok(name.into()),
                    Err(_) => match Grammar::match_url(s) {
                        Some(_) => match Url::from_str(s) {
                            Ok(url) => Ok(url.into()),
                            Err(_) => Err(Self::Err::new(s)),
                        },
                        None => Err(Self::Err::new(s)),
                    },
                },
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(DatasetRefAny);

crate::formats::impl_invalid_value!(DatasetRefAny);

impl fmt::Display for DatasetRefAny {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatasetRefAny::ID(v) => write!(f, "{}", v),
            DatasetRefAny::Name(v) => write!(f, "{}", v),
            DatasetRefAny::Handle(v) => write!(f, "{}", v),
            DatasetRefAny::RemoteName(v) => write!(f, "{}", v),
            DatasetRefAny::RemoteHandle(v) => write!(f, "{}", v),
            DatasetRefAny::Url(v) => write!(f, "{}", v),
        }
    }
}

impl From<DatasetID> for DatasetRefAny {
    fn from(v: DatasetID) -> Self {
        Self::ID(v)
    }
}

impl From<&DatasetID> for DatasetRefAny {
    fn from(v: &DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<DatasetName> for DatasetRefAny {
    fn from(v: DatasetName) -> Self {
        Self::Name(v)
    }
}

impl From<&DatasetName> for DatasetRefAny {
    fn from(v: &DatasetName) -> Self {
        Self::Name(v.clone())
    }
}

impl From<DatasetHandle> for DatasetRefAny {
    fn from(v: DatasetHandle) -> Self {
        Self::Handle(v)
    }
}

impl From<&DatasetHandle> for DatasetRefAny {
    fn from(v: &DatasetHandle) -> Self {
        Self::Handle(v.clone())
    }
}

impl From<RemoteDatasetName> for DatasetRefAny {
    fn from(v: RemoteDatasetName) -> Self {
        Self::RemoteName(v)
    }
}

impl From<&RemoteDatasetName> for DatasetRefAny {
    fn from(v: &RemoteDatasetName) -> Self {
        Self::RemoteName(v.clone())
    }
}

impl From<RemoteDatasetHandle> for DatasetRefAny {
    fn from(v: RemoteDatasetHandle) -> Self {
        Self::RemoteHandle(v)
    }
}

impl From<&RemoteDatasetHandle> for DatasetRefAny {
    fn from(v: &RemoteDatasetHandle) -> Self {
        Self::RemoteHandle(v.clone())
    }
}

impl From<DatasetRefLocal> for DatasetRefAny {
    fn from(v: DatasetRefLocal) -> Self {
        match v {
            DatasetRefLocal::ID(v) => DatasetRefAny::ID(v),
            DatasetRefLocal::Name(v) => DatasetRefAny::Name(v),
            DatasetRefLocal::Handle(v) => DatasetRefAny::Handle(v),
        }
    }
}

impl From<&DatasetRefLocal> for DatasetRefAny {
    fn from(v: &DatasetRefLocal) -> Self {
        match v {
            DatasetRefLocal::ID(v) => DatasetRefAny::ID(v.clone()),
            DatasetRefLocal::Name(v) => DatasetRefAny::Name(v.clone()),
            DatasetRefLocal::Handle(v) => DatasetRefAny::Handle(v.clone()),
        }
    }
}

impl From<DatasetRefRemote> for DatasetRefAny {
    fn from(v: DatasetRefRemote) -> Self {
        match v {
            DatasetRefRemote::ID(v) => DatasetRefAny::ID(v),
            DatasetRefRemote::RemoteName(v) => DatasetRefAny::RemoteName(v),
            DatasetRefRemote::RemoteHandle(v) => DatasetRefAny::RemoteHandle(v),
            DatasetRefRemote::Url(v) => DatasetRefAny::Url(v),
        }
    }
}

impl From<&DatasetRefRemote> for DatasetRefAny {
    fn from(v: &DatasetRefRemote) -> Self {
        match v {
            DatasetRefRemote::ID(v) => DatasetRefAny::ID(v.clone()),
            DatasetRefRemote::RemoteName(v) => DatasetRefAny::RemoteName(v.clone()),
            DatasetRefRemote::RemoteHandle(v) => DatasetRefAny::RemoteHandle(v.clone()),
            DatasetRefRemote::Url(v) => DatasetRefAny::Url(v.clone()),
        }
    }
}

impl From<Url> for DatasetRefAny {
    fn from(v: Url) -> Self {
        Self::Url(Arc::new(v))
    }
}

////////////////////////////////////////////////////////////////////////////////
// RepoDatasetRef
////////////////////////////////////////////////////////////////////////////////

impl RepoDatasetRef {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            RepoDatasetRef::ID(id) => Some(id),
            RepoDatasetRef::Name(_) => None,
            RepoDatasetRef::Handle(RepoDatasetHandle { id, .. }) => Some(id),
            RepoDatasetRef::Url(_) => None,
        }
    }

    pub fn name(&self) -> Option<&DatasetNameWithOwner> {
        match self {
            RepoDatasetRef::ID(_) => None,
            RepoDatasetRef::Name(name) => Some(name),
            RepoDatasetRef::Handle(RepoDatasetHandle { name, .. }) => Some(name),
            RepoDatasetRef::Url(_) => None,
        }
    }
}

impl std::str::FromStr for RepoDatasetRef {
    type Err = InvalidValue<RepoDatasetRef>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetID::from_str(s) {
            Ok(id) => Ok(id.into()),
            Err(_) => match DatasetNameWithOwner::from_str(s) {
                Ok(name) => Ok(name.into()),
                Err(_) => match Grammar::match_url(s) {
                    Some(_) => match Url::from_str(s) {
                        Ok(url) => Ok(url.into()),
                        Err(_) => Err(Self::Err::new(s)),
                    },
                    None => Err(Self::Err::new(s)),
                },
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(RepoDatasetRef);

crate::formats::impl_invalid_value!(RepoDatasetRef);

impl fmt::Display for RepoDatasetRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepoDatasetRef::ID(v) => write!(f, "{}", v),
            RepoDatasetRef::Name(v) => write!(f, "{}", v),
            RepoDatasetRef::Handle(v) => write!(f, "{}", v),
            RepoDatasetRef::Url(v) => write!(f, "{}", v),
        }
    }
}

impl From<DatasetID> for RepoDatasetRef {
    fn from(v: DatasetID) -> Self {
        Self::ID(v)
    }
}

impl From<&DatasetID> for RepoDatasetRef {
    fn from(v: &DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<DatasetNameWithOwner> for RepoDatasetRef {
    fn from(v: DatasetNameWithOwner) -> Self {
        Self::Name(v)
    }
}

impl From<&DatasetNameWithOwner> for RepoDatasetRef {
    fn from(v: &DatasetNameWithOwner) -> Self {
        Self::Name(v.clone())
    }
}

impl From<RepoDatasetHandle> for RepoDatasetRef {
    fn from(v: RepoDatasetHandle) -> Self {
        Self::Handle(v)
    }
}

impl From<&RepoDatasetHandle> for RepoDatasetRef {
    fn from(v: &RepoDatasetHandle) -> Self {
        Self::Handle(v.clone())
    }
}

impl From<Url> for RepoDatasetRef {
    fn from(v: Url) -> Self {
        Self::Url(Arc::new(v))
    }
}
