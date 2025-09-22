// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use thiserror::Error;
use url::Url;

use super::grammar::Grammar;
use super::*;
use crate::formats::ParseError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// References local dataset by ID or alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRef {
    ID(DatasetID),
    Alias(DatasetAlias),
    Handle(DatasetHandle),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// References remote dataset by ID, URL, or alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRefRemote {
    ID(Option<RepoName>, DatasetID),
    Alias(DatasetAliasRemote),
    Url(Arc<Url>),
    Handle(DatasetHandleRemote),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// References any dataset, local or remote.
///
/// Note that when interpreting reference such as `"foo/bar"` there is an
/// ambiguity between treating it as `<account>/<dataset>` or
/// `<repo>/<dataset>`. That's why this reference needs to be disambiguated
/// before accessing those parameters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasetRefAny {
    ID(Option<RepoName>, DatasetID),
    RemoteAlias(RepoName, Option<AccountName>, DatasetName),
    LocalAlias(Option<AccountName>, DatasetName),
    AmbiguousAlias(Arc<str>, DatasetName),
    Url(Arc<Url>),
    LocalHandle(DatasetHandle),
    RemoteHandle(DatasetHandleRemote),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRef {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            Self::ID(id) | Self::Handle(DatasetHandle { id, .. }) => Some(id),
            Self::Alias(_) => None,
        }
    }

    pub fn into_id(self) -> Option<DatasetID> {
        match self {
            Self::ID(id) | Self::Handle(DatasetHandle { id, .. }) => Some(id),
            Self::Alias(_) => None,
        }
    }

    pub fn alias(&self) -> Option<&DatasetAlias> {
        match self {
            Self::ID(_) => None,
            Self::Alias(alias) | Self::Handle(DatasetHandle { alias, .. }) => Some(alias),
        }
    }

    pub fn account_name(&self) -> Option<&AccountName> {
        match self {
            Self::ID(_) => None,
            Self::Alias(alias) | Self::Handle(DatasetHandle { alias, .. }) => {
                alias.account_name.as_ref()
            }
        }
    }

    pub fn dataset_name(&self) -> Option<&DatasetName> {
        match self {
            Self::ID(_) => None,
            Self::Alias(alias) | Self::Handle(DatasetHandle { alias, .. }) => {
                Some(&alias.dataset_name)
            }
        }
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

impl std::str::FromStr for DatasetRef {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetID::from_did_str(s) {
            Ok(id) => Ok(Self::ID(id)),
            Err(_) => match DatasetAlias::from_str(s) {
                Ok(alias) => Ok(Self::Alias(alias)),
                Err(_) => Err(Self::Err::new(s)),
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(DatasetRef);

super::dataset_identity::impl_parse_error!(DatasetRef);

impl fmt::Display for DatasetRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ID(v) => write!(f, "{}", v.as_did_str()),
            Self::Alias(v) => write!(f, "{v}"),
            Self::Handle(v) => write!(f, "{v}"),
        }
    }
}

impl_serde!(DatasetRef, DatasetRefSerdeVisitor);

impl From<DatasetID> for DatasetRef {
    fn from(v: DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<&DatasetID> for DatasetRef {
    fn from(v: &DatasetID) -> Self {
        Self::ID(v.clone())
    }
}

impl From<DatasetName> for DatasetRef {
    fn from(v: DatasetName) -> Self {
        Self::Alias(DatasetAlias::new(None, v))
    }
}

impl From<&DatasetName> for DatasetRef {
    fn from(v: &DatasetName) -> Self {
        Self::Alias(DatasetAlias::new(None, v.clone()))
    }
}

impl From<DatasetAlias> for DatasetRef {
    fn from(v: DatasetAlias) -> Self {
        Self::Alias(v)
    }
}

impl From<&DatasetAlias> for DatasetRef {
    fn from(v: &DatasetAlias) -> Self {
        Self::Alias(v.clone())
    }
}

impl From<DatasetHandle> for DatasetRef {
    fn from(v: DatasetHandle) -> Self {
        Self::Handle(v)
    }
}

impl From<&DatasetHandle> for DatasetRef {
    fn from(v: &DatasetHandle) -> Self {
        Self::Handle(v.clone())
    }
}

impl Ord for DatasetRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let l = (self.account_name(), self.dataset_name(), self.id());
        let r = (other.account_name(), other.dataset_name(), other.id());
        l.cmp(&r)
    }
}

impl PartialOrd for DatasetRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefRemote
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRefRemote {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            Self::ID(_, id) => Some(id),
            Self::Alias(_) | Self::Url(_) => None,
            Self::Handle(hdl) => Some(&hdl.id),
        }
    }

    pub fn alias(&self) -> Option<&DatasetAliasRemote> {
        match self {
            Self::ID(_, _) | Self::Url(_) => None,
            Self::Alias(alias) => Some(alias),
            Self::Handle(hdl) => Some(&hdl.alias),
        }
    }

    pub fn url(&self) -> Option<&Url> {
        match self {
            Self::ID(_, _) | Self::Alias(_) | Self::Handle(_) => None,
            Self::Url(url) => Some(url.as_ref()),
        }
    }

    pub fn dataset_name(&self) -> Option<&DatasetName> {
        match self {
            Self::ID(_, _) | Self::Url(_) => None,
            Self::Alias(alias) => Some(&alias.dataset_name),
            Self::Handle(hdl) => Some(&hdl.alias.dataset_name),
        }
    }

    pub fn account_name(&self) -> Option<&AccountName> {
        match self {
            Self::ID(_, _) | Self::Url(_) => None,
            Self::Alias(alias) => alias.account_name.as_ref(),
            Self::Handle(hdl) => hdl.alias.account_name.as_ref(),
        }
    }

    pub fn repo_name(&self) -> Option<&RepoName> {
        match self {
            Self::ID(repo_name, _) => repo_name.as_ref(),
            Self::Alias(alias) => Some(&alias.repo_name),
            Self::Url(_) => None,
            Self::Handle(hdl) => Some(&hdl.alias.repo_name),
        }
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

impl std::str::FromStr for DatasetRefRemote {
    type Err = ParseError<DatasetRefRemote>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_remote_dataset_id(s) {
            Some((repo, id, "")) => match DatasetID::from_did_str(id) {
                Ok(id) => Ok(Self::ID(repo.map(RepoName::new_unchecked), id)),
                Err(_) => Err(Self::Err::new(s)),
            },
            _ => match DatasetAliasRemote::from_str(s) {
                Ok(alias) => Ok(Self::Alias(alias)),
                Err(_) => match Grammar::match_url(s) {
                    Some(_) => match Url::from_str(s) {
                        Ok(url) => Ok(Self::Url(Arc::new(url))),
                        Err(_) => Err(Self::Err::new(s)),
                    },
                    None => Err(Self::Err::new(s)),
                },
            },
        }
    }
}

super::dataset_identity::impl_try_from_str!(DatasetRefRemote);

super::dataset_identity::impl_parse_error!(DatasetRefRemote);

impl fmt::Display for DatasetRefRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatasetRefRemote::ID(None, id) => write!(f, "{}", id.as_did_str()),
            DatasetRefRemote::ID(Some(repo), id) => write!(f, "{}/{}", repo, id.as_did_str()),
            DatasetRefRemote::Alias(v) => write!(f, "{v}"),
            DatasetRefRemote::Url(v) => write!(f, "{v}"),
            DatasetRefRemote::Handle(v) => write!(f, "{v}"),
        }
    }
}

impl_serde!(DatasetRefRemote, DatasetRefRemoteSerdeVisitor);

impl From<DatasetID> for DatasetRefRemote {
    fn from(v: DatasetID) -> Self {
        Self::ID(None, v)
    }
}

impl From<&DatasetID> for DatasetRefRemote {
    fn from(v: &DatasetID) -> Self {
        Self::ID(None, v.clone())
    }
}

impl From<DatasetAliasRemote> for DatasetRefRemote {
    fn from(v: DatasetAliasRemote) -> Self {
        Self::Alias(v)
    }
}

impl From<&DatasetAliasRemote> for DatasetRefRemote {
    fn from(v: &DatasetAliasRemote) -> Self {
        Self::Alias(v.clone())
    }
}

impl From<DatasetHandleRemote> for DatasetRefRemote {
    fn from(v: DatasetHandleRemote) -> Self {
        Self::Handle(v)
    }
}

impl From<&DatasetHandleRemote> for DatasetRefRemote {
    fn from(v: &DatasetHandleRemote) -> Self {
        Self::Handle(v.clone())
    }
}

impl From<&Url> for DatasetRefRemote {
    fn from(v: &Url) -> Self {
        Self::Url(Arc::new(v.clone()))
    }
}

impl From<Url> for DatasetRefRemote {
    fn from(v: Url) -> Self {
        Self::Url(Arc::new(v))
    }
}

impl std::cmp::Ord for DatasetRefRemote {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let l = (self.url(), self.alias(), self.repo_name(), self.id());
        let r = (self.url(), other.alias(), self.repo_name(), other.id());
        l.cmp(&r)
    }
}

impl std::cmp::PartialOrd for DatasetRefRemote {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefAny
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRefAny {
    pub fn id(&self) -> Option<&DatasetID> {
        match self {
            Self::ID(_, id) => Some(id),
            _ => None,
        }
    }

    pub fn as_local_ref(
        &self,
        is_repo: impl Fn(&RepoName) -> bool,
    ) -> Result<DatasetRef, DatasetRefRemote> {
        self.clone().into_local_ref(is_repo)
    }

    pub fn as_local_single_tenant_ref(&self) -> Result<DatasetRef, DatasetRefRemote> {
        self.as_local_ref(|_| true)
    }

    pub fn into_local_single_tenant_ref(self) -> Result<DatasetRef, DatasetRefRemote> {
        self.into_local_ref(|_| true)
    }

    pub fn into_local_ref(
        self,
        is_repo: impl Fn(&RepoName) -> bool,
    ) -> Result<DatasetRef, DatasetRefRemote> {
        match self {
            Self::ID(None, id) => Ok(DatasetRef::ID(id)),
            Self::ID(Some(repo), id) => Err(DatasetRefRemote::ID(Some(repo), id)),
            Self::LocalAlias(account_name, dataset_name) => Ok(DatasetRef::Alias(
                DatasetAlias::new(account_name, dataset_name),
            )),
            Self::RemoteAlias(repo_name, account_name, dataset_name) => {
                Err(DatasetRefRemote::Alias(DatasetAliasRemote {
                    repo_name,
                    account_name,
                    dataset_name,
                }))
            }
            Self::AmbiguousAlias(prefix, dataset_name) => {
                let repo_name = RepoName::from_inner_unchecked(prefix);
                if is_repo(&repo_name) {
                    Err(DatasetRefRemote::Alias(DatasetAliasRemote {
                        repo_name,
                        account_name: None,
                        dataset_name,
                    }))
                } else {
                    Ok(DatasetRef::Alias(DatasetAlias {
                        account_name: Some(AccountName::from_inner_unchecked(
                            repo_name.into_inner(),
                        )),
                        dataset_name,
                    }))
                }
            }
            Self::Url(url) => Err(DatasetRefRemote::Url(url)),
            Self::LocalHandle(hdl) => Ok(DatasetRef::Handle(hdl)),
            Self::RemoteHandle(hdl) => Err(DatasetRefRemote::Handle(hdl)),
        }
    }

    pub fn as_remote_ref(
        &self,
        is_repo: impl Fn(&RepoName) -> bool,
    ) -> Result<DatasetRefRemote, DatasetRef> {
        self.clone().into_remote_ref(is_repo)
    }

    pub fn into_remote_ref(
        self,
        is_repo: impl Fn(&RepoName) -> bool,
    ) -> Result<DatasetRefRemote, DatasetRef> {
        match self {
            Self::ID(repo, id) => Ok(DatasetRefRemote::ID(repo, id)),
            Self::LocalAlias(account_name, dataset_name) => Err(DatasetRef::Alias(
                DatasetAlias::new(account_name, dataset_name),
            )),
            Self::RemoteAlias(repo_name, account_name, dataset_name) => {
                Ok(DatasetRefRemote::Alias(DatasetAliasRemote {
                    repo_name,
                    account_name,
                    dataset_name,
                }))
            }
            Self::AmbiguousAlias(prefix, dataset_name) => {
                let repo_name = RepoName::from_inner_unchecked(prefix);
                if is_repo(&repo_name) {
                    Ok(DatasetRefRemote::Alias(DatasetAliasRemote {
                        repo_name,
                        account_name: None,
                        dataset_name,
                    }))
                } else {
                    Err(DatasetRef::Alias(DatasetAlias {
                        account_name: Some(AccountName::from_inner_unchecked(
                            repo_name.into_inner(),
                        )),
                        dataset_name,
                    }))
                }
            }
            Self::Url(url) => Ok(DatasetRefRemote::Url(url)),
            Self::LocalHandle(hdl) => Err(DatasetRef::Handle(hdl)),
            Self::RemoteHandle(hdl) => Ok(DatasetRefRemote::Handle(hdl)),
        }
    }
}

impl fmt::Display for DatasetRefAny {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ID(None, id) => write!(f, "{}", id.as_did_str()),
            Self::ID(Some(repo), id) => write!(f, "{}/{}", repo, id.as_did_str()),
            Self::LocalAlias(None, name) => write!(f, "{name}"),
            Self::LocalAlias(Some(account), name) => write!(f, "{account}/{name}"),
            Self::RemoteAlias(repo, None, name) => write!(f, "{repo}/{name}"),
            Self::RemoteAlias(repo, Some(account), name) => {
                write!(f, "{repo}/{account}/{name}")
            }
            Self::AmbiguousAlias(repo_or_account, name) => {
                write!(f, "{repo_or_account}/{name}")
            }
            Self::Url(v) => write!(f, "{v}"),
            Self::LocalHandle(v) => write!(f, "{v}"),
            Self::RemoteHandle(v) => write!(f, "{v}"),
        }
    }
}

impl std::str::FromStr for DatasetRefAny {
    type Err = ParseError<DatasetRefAny>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Grammar::match_remote_dataset_id(s) {
            Some((repo, id, "")) => match DatasetID::from_did_str(id) {
                Ok(id) => Ok(Self::ID(repo.map(RepoName::new_unchecked), id)),
                Err(_) => Err(Self::Err::new(s)),
            },
            _ => match DatasetName::from_str(s) {
                Ok(dataset_name) => Ok(Self::LocalAlias(None, dataset_name)),
                Err(_) => match DatasetAliasRemote::from_str(s) {
                    Ok(alias) => {
                        if let Some(account_name) = alias.account_name {
                            Ok(Self::RemoteAlias(
                                alias.repo_name,
                                Some(account_name),
                                alias.dataset_name,
                            ))
                        } else {
                            Ok(Self::AmbiguousAlias(
                                alias.repo_name.into_inner(),
                                alias.dataset_name,
                            ))
                        }
                    }
                    Err(_) => match Grammar::match_url(s) {
                        Some(_) => match Url::from_str(s) {
                            Ok(url) => Ok(Self::Url(Arc::new(url))),
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

super::dataset_identity::impl_parse_error!(DatasetRefAny);

impl_serde!(DatasetRefAny, DatasetRefAnySerdeVisitor);

impl From<DatasetID> for DatasetRefAny {
    fn from(v: DatasetID) -> Self {
        Self::ID(None, v)
    }
}

impl From<&DatasetID> for DatasetRefAny {
    fn from(v: &DatasetID) -> Self {
        Self::ID(None, v.clone())
    }
}

impl From<DatasetName> for DatasetRefAny {
    fn from(v: DatasetName) -> Self {
        Self::LocalAlias(None, v)
    }
}

impl From<&DatasetName> for DatasetRefAny {
    fn from(v: &DatasetName) -> Self {
        Self::LocalAlias(None, v.clone())
    }
}

impl From<DatasetAlias> for DatasetRefAny {
    fn from(v: DatasetAlias) -> Self {
        Self::LocalAlias(v.account_name, v.dataset_name)
    }
}

impl From<&DatasetAlias> for DatasetRefAny {
    fn from(v: &DatasetAlias) -> Self {
        Self::LocalAlias(v.account_name.clone(), v.dataset_name.clone())
    }
}

impl From<DatasetAliasRemote> for DatasetRefAny {
    fn from(v: DatasetAliasRemote) -> Self {
        Self::RemoteAlias(v.repo_name, v.account_name, v.dataset_name)
    }
}

impl From<&DatasetAliasRemote> for DatasetRefAny {
    fn from(v: &DatasetAliasRemote) -> Self {
        Self::RemoteAlias(
            v.repo_name.clone(),
            v.account_name.clone(),
            v.dataset_name.clone(),
        )
    }
}

impl From<&Url> for DatasetRefAny {
    fn from(v: &Url) -> Self {
        Self::Url(Arc::new(v.clone()))
    }
}

impl From<Url> for DatasetRefAny {
    fn from(v: Url) -> Self {
        Self::Url(Arc::new(v))
    }
}

impl From<DatasetRef> for DatasetRefAny {
    fn from(v: DatasetRef) -> Self {
        match v {
            DatasetRef::ID(v) => Self::ID(None, v),
            DatasetRef::Alias(v) => Self::LocalAlias(v.account_name, v.dataset_name),
            DatasetRef::Handle(v) => Self::LocalHandle(v),
        }
    }
}

impl From<&DatasetRef> for DatasetRefAny {
    fn from(v: &DatasetRef) -> Self {
        match v {
            DatasetRef::ID(v) => Self::ID(None, v.clone()),
            DatasetRef::Alias(v) => {
                Self::LocalAlias(v.account_name.clone(), v.dataset_name.clone())
            }
            DatasetRef::Handle(v) => Self::LocalHandle(v.clone()),
        }
    }
}

impl From<DatasetRefRemote> for DatasetRefAny {
    fn from(v: DatasetRefRemote) -> Self {
        match v {
            DatasetRefRemote::ID(repo, id) => Self::ID(repo, id),
            DatasetRefRemote::Alias(v) => {
                Self::RemoteAlias(v.repo_name, v.account_name, v.dataset_name)
            }
            DatasetRefRemote::Url(v) => Self::Url(v),
            DatasetRefRemote::Handle(v) => Self::RemoteHandle(v),
        }
    }
}

impl From<&DatasetRefRemote> for DatasetRefAny {
    fn from(v: &DatasetRefRemote) -> Self {
        match v {
            DatasetRefRemote::ID(repo, id) => Self::ID(repo.clone(), id.clone()),
            DatasetRefRemote::Alias(v) => Self::RemoteAlias(
                v.repo_name.clone(),
                v.account_name.clone(),
                v.dataset_name.clone(),
            ),
            DatasetRefRemote::Url(v) => Self::Url(v.clone()),
            DatasetRefRemote::Handle(v) => Self::RemoteHandle(v.clone()),
        }
    }
}

impl From<DatasetHandle> for DatasetRefAny {
    fn from(v: DatasetHandle) -> Self {
        Self::LocalHandle(v)
    }
}

impl From<&DatasetHandle> for DatasetRefAny {
    fn from(v: &DatasetHandle) -> Self {
        Self::LocalHandle(v.clone())
    }
}

impl From<DatasetHandleRemote> for DatasetRefAny {
    fn from(v: DatasetHandleRemote) -> Self {
        Self::RemoteHandle(v)
    }
}

impl From<&DatasetHandleRemote> for DatasetRefAny {
    fn from(v: &DatasetHandleRemote) -> Self {
        Self::RemoteHandle(v.clone())
    }
}

impl std::cmp::Ord for DatasetRefAny {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        #[allow(clippy::type_complexity)]
        fn tuplify(
            v: &DatasetRefAny,
        ) -> (
            Option<&str>,
            Option<&str>,
            Option<&DatasetName>,
            Option<&DatasetID>,
            Option<&Url>,
        ) {
            match v {
                DatasetRefAny::ID(r, id) => {
                    (r.as_ref().map(RepoName::as_str), None, None, Some(id), None)
                }
                DatasetRefAny::LocalAlias(a, n) => (
                    None,
                    a.as_ref().map(AccountName::as_str),
                    Some(n),
                    None,
                    None,
                ),
                DatasetRefAny::RemoteAlias(r, a, n) => (
                    Some(r.as_ref()),
                    a.as_ref().map(AccountName::as_str),
                    Some(n),
                    None,
                    None,
                ),
                DatasetRefAny::AmbiguousAlias(ra, n) => {
                    (Some(ra.as_ref()), None, Some(n), None, None)
                }
                DatasetRefAny::Url(url) => (None, None, None, None, Some(url.as_ref())),
                DatasetRefAny::LocalHandle(hdl) => (
                    None,
                    hdl.alias.account_name.as_ref().map(AccountName::as_str),
                    Some(&hdl.alias.dataset_name),
                    Some(&hdl.id),
                    None,
                ),
                DatasetRefAny::RemoteHandle(hdl) => (
                    Some(hdl.alias.repo_name.as_str()),
                    hdl.alias.account_name.as_ref().map(AccountName::as_str),
                    Some(&hdl.alias.dataset_name),
                    Some(&hdl.id),
                    None,
                ),
            }
        }
        let l = tuplify(self);
        let r = tuplify(other);
        l.cmp(&r)
    }
}

impl std::cmp::PartialOrd for DatasetRefAny {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefPattern
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DatasetRefPattern {
    Ref(DatasetRef),
    Pattern(DatasetAliasPattern),
}

impl DatasetRefPattern {
    pub fn matches(&self, dataset_handle: &DatasetHandle) -> bool {
        match self {
            Self::Ref(dataset_ref) => match dataset_ref {
                DatasetRef::ID(dataset_id) => *dataset_id == dataset_handle.id,
                DatasetRef::Alias(dataset_alias) => *dataset_alias == dataset_handle.alias,
                DatasetRef::Handle(dataset_handle_res) => dataset_handle_res == dataset_handle,
            },
            Self::Pattern(dataset_pattern) => dataset_pattern.matches(dataset_handle),
        }
    }

    /// Convert into a [`DatasetRef`] when value is not a glob pattern
    pub fn as_dataset_ref(&self) -> Option<&DatasetRef> {
        match self {
            Self::Pattern(_) => None,
            Self::Ref(dataset_ref) => Some(dataset_ref),
        }
    }

    /// Returns `false` if value is a reference to some specific dataset rather
    /// than a glob pattern
    pub fn is_pattern(&self) -> bool {
        matches!(self, Self::Pattern(_))
    }
}

impl std::str::FromStr for DatasetRefPattern {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('%') {
            return match DatasetAliasPattern::from_str(s) {
                Ok(dataset_pattern) => Ok(Self::Pattern(dataset_pattern)),
                Err(_) => Err(Self::Err::new(s)),
            };
        }
        match DatasetRef::from_str(s) {
            Ok(dataset_ref) => Ok(Self::Ref(dataset_ref)),
            Err(_) => Err(Self::Err::new(s)),
        }
    }
}

impl fmt::Display for DatasetRefPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ref(dataset_ref) => write!(f, "{dataset_ref}"),
            Self::Pattern(dataset_pattern) => {
                if let Some(account_name) = &dataset_pattern.account_name {
                    write!(f, "{account_name}/")?;
                }
                write!(f, "{}", dataset_pattern.dataset_name_pattern)
            }
        }
    }
}

super::dataset_identity::impl_parse_error!(DatasetRefPattern);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DatasetRefAnyPattern {
    Ref(DatasetRefAny),
    PatternRemote(RepoName, AccountName, DatasetNamePattern),
    PatternAmbiguous(DatasetAmbiguousPattern, DatasetNamePattern),
    PatternLocal(DatasetNamePattern),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetAmbiguousPattern {
    pub pattern: Arc<str>,
}

impl DatasetRefAnyPattern {
    /// Convert into a [`DatasetRefAny`] pointer when value is not a glob
    /// pattern
    pub fn as_dataset_ref_any(&self) -> Option<&DatasetRefAny> {
        match self {
            Self::Ref(dataset_ref) => Some(dataset_ref),
            Self::PatternLocal(_) | Self::PatternAmbiguous(_, _) | Self::PatternRemote(_, _, _) => {
                None
            }
        }
    }

    /// Convert into a [`DatasetRefAny`] when value is not a glob pattern
    pub fn into_dataset_ref_any(self) -> Option<DatasetRefAny> {
        match self {
            Self::Ref(dataset_ref) => Some(dataset_ref),
            Self::PatternLocal(_) | Self::PatternAmbiguous(_, _) | Self::PatternRemote(_, _, _) => {
                None
            }
        }
    }

    /// Returns `false` if value is a reference to some specific dataset rather
    /// than a glob pattern
    pub fn is_pattern(&self) -> bool {
        !matches!(self, Self::Ref(_))
    }

    /// Return `true` if pattern has remote repo reference
    pub fn is_remote_pattern(&self, is_multitenant_mode: bool) -> bool {
        match self {
            Self::Ref(_) | Self::PatternLocal(_) => false,
            Self::PatternRemote(_, _, _) => true,
            Self::PatternAmbiguous(_, _) => !is_multitenant_mode,
        }
    }

    /// Return repository part from pattern
    pub fn pattern_repo_name(&self, is_multitenant_mode: bool) -> Option<RepoName> {
        match self {
            Self::Ref(_) | Self::PatternLocal(_) => None,
            Self::PatternRemote(repo_name, _, _) => Some(repo_name.clone()),
            Self::PatternAmbiguous(account_repo_name, _) => {
                if is_multitenant_mode {
                    return None;
                }
                Some(RepoName::from_str(&account_repo_name.pattern).unwrap())
            }
        }
    }
}

impl std::str::FromStr for DatasetRefAnyPattern {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('%') {
            return match s.split_once('/') {
                Some((repo, rest)) => match RepoName::try_from(repo) {
                    Ok(repo_or_account_name) => match rest.split_once('/') {
                        Some((account_pattern, dataset_name)) => {
                            let repo_name = repo_or_account_name;
                            match AccountName::from_str(account_pattern) {
                                Ok(account_name) => {
                                    match DatasetNamePattern::from_str(dataset_name) {
                                        Ok(dataset_name_pattern) => Ok(Self::PatternRemote(
                                            repo_name,
                                            account_name,
                                            dataset_name_pattern,
                                        )),
                                        Err(_) => Err(Self::Err::new(s)),
                                    }
                                }
                                Err(_) => Err(Self::Err::new(s)),
                            }
                        }
                        None => match DatasetNamePattern::from_str(rest) {
                            Ok(dataset_name_pattern) => Ok(Self::PatternAmbiguous(
                                DatasetAmbiguousPattern {
                                    pattern: repo_or_account_name.into_inner(),
                                },
                                dataset_name_pattern,
                            )),
                            Err(_) => Err(Self::Err::new(s)),
                        },
                    },
                    Err(_) => Err(Self::Err::new(s)),
                },
                None => match DatasetNamePattern::try_from(s) {
                    Ok(dataset_name_pattern) => Ok(Self::PatternLocal(dataset_name_pattern)),
                    Err(_) => Err(Self::Err::new(s)),
                },
            };
        }
        match DatasetRefAny::from_str(s) {
            Ok(dataset_ref) => Ok(Self::Ref(dataset_ref)),
            Err(_) => Err(Self::Err::new(s)),
        }
    }
}

super::dataset_identity::impl_parse_error!(DatasetRefAnyPattern);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetPushTarget {
    Url(Url),
    Alias(DatasetAliasRemote),
    Repository(RepoName),
}

impl std::str::FromStr for DatasetPushTarget {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DatasetAliasRemote::from_str(s) {
            Ok(alias) => Ok(Self::Alias(alias)),
            Err(_) => match RepoName::from_str(s) {
                Ok(repo_name) => Ok(Self::Repository(repo_name)),
                Err(_) => match Grammar::match_url(s) {
                    Some(_) => match Url::from_str(s) {
                        Ok(url) => Ok(Self::Url(url)),
                        Err(_) => Err(Self::Err::new(s)),
                    },
                    None => Err(Self::Err::new(s)),
                },
            },
        }
    }
}

impl DatasetPushTarget {
    pub fn into_repo_name(self) -> Option<RepoName> {
        match self {
            Self::Alias(dataset_alias_remote) => Some(dataset_alias_remote.repo_name),
            Self::Repository(repo_name) => Some(repo_name),
            Self::Url(_) => None,
        }
    }
}

impl fmt::Display for DatasetPushTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Alias(v) => write!(f, "{v}"),
            Self::Url(v) => write!(f, "{v}"),
            Self::Repository(v) => write!(f, "{v}"),
        }
    }
}

impl TryFrom<DatasetRefRemote> for DatasetPushTarget {
    type Error = DatasetPushTargetError;

    fn try_from(value: DatasetRefRemote) -> Result<Self, Self::Error> {
        match value {
            DatasetRefRemote::Alias(remote_alias_ref) => Ok(Self::Alias(remote_alias_ref)),
            DatasetRefRemote::Url(url_ref) => Ok(Self::Url(url_ref.as_ref().clone())),
            _ => Err(Self::Error::UnsupportedType),
        }
    }
}

#[derive(Error, Debug)]
pub enum DatasetPushTargetError {
    #[error("Unsupported type to cast")]
    UnsupportedType,
}

super::dataset_identity::impl_parse_error!(DatasetPushTarget);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
