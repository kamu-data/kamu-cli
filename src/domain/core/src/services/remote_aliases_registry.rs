// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;

use crate::*;

#[async_trait]
pub trait RemoteAliasesRegistry: Send + Sync {
    async fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError>;
}

#[derive(Error, Debug)]
pub enum GetAliasesError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for GetAliasesError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasResolver
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait RemoteAliasResolver: Send + Sync {
    // Resolve remote push target.
    // Firstly try to resolve from AliasRegistry, if cannot do it
    // try to resolve via repository registry
    async fn resolve_push_target(
        &self,
        local_dataset_handle: &DatasetHandle,
        dataset_push_target_maybe: Option<DatasetPushTarget>,
    ) -> Result<RemoteTarget, ResolveAliasError>;
}

#[derive(Debug, Clone)]
pub struct RemoteTarget {
    pub url: url::Url,
    pub repo_name: Option<RepoName>,
    pub dataset_name: Option<DatasetName>,
    pub account_name: Option<AccountName>,
}

impl RemoteTarget {
    pub fn new(
        url: url::Url,
        repo_name: Option<RepoName>,
        dataset_name: Option<DatasetName>,
        account_name: Option<AccountName>,
    ) -> Self {
        Self {
            url,
            repo_name,
            dataset_name,
            account_name,
        }
    }
}

#[derive(Error, Debug)]
pub enum ResolveAliasError {
    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),
    #[error("Cannot choose between multiple repositories")]
    AmbiguousRepository,
    #[error("Cannot choose between multiple push aliases")]
    AmbiguousAlias,
    #[error("Repositories list is empty")]
    EmptyRepositoryList,
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<ResolveAliasError> for PushError {
    fn from(val: ResolveAliasError) -> Self {
        match val {
            ResolveAliasError::AmbiguousAlias | ResolveAliasError::AmbiguousRepository => {
                Self::AmbiguousTarget
            }
            ResolveAliasError::EmptyRepositoryList | ResolveAliasError::RepositoryNotFound(_) => {
                Self::NoTarget
            }
            ResolveAliasError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetRemoteAccountError {
    #[error(transparent)]
    InvalidResponse(InvalidApiResponseError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Invalid gql response: {response}")]
pub struct InvalidApiResponseError {
    pub response: String,
}

impl From<InternalError> for GetRemoteAccountError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}
