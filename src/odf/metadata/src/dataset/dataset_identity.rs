// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use like::ILike;
use multiformats::ParseError;

use super::*;
use crate::auth::AccountName;
use crate::formats::*;
use crate::storage::RepoName;

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

impl std::fmt::Display for DatasetAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for DatasetAliasRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
