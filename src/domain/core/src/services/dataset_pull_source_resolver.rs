// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{RepositoryNotFoundError, ResolveAliasError, SyncError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Finds the remote a local dataset is pulled from.
///
/// A dataset with one pull alias is pulled from it. Several pull aliases are
/// ambiguous.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetPullSourceResolver: Send + Sync {
    /// The dataset's pull alias. Reads the alias registry only
    async fn find_pull_alias(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<odf::DatasetRefRemote>, FindPullAliasError>;

    /// The dataset's pull alias with the URL to pull from
    async fn resolve_pull_source(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<DatasetPullSource>, ResolvePullSourceError>;

    /// The URL to pull a remote reference from. A repository on the ODF
    /// protocol may be asked for the account when the reference names none
    async fn resolve_remote_source(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<DatasetPullSource, ResolveRemoteSourceError>;

    /// The current account's dataset that has this pull alias
    async fn find_dataset_by_pull_alias(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<Option<odf::DatasetHandle>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A remote a local dataset is pulled from, with the URL it resolves to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetPullSource {
    pub remote_ref: odf::DatasetRefRemote,
    pub url: url::Url,
}

impl DatasetPullSource {
    /// The endpoint a sync task reads, without credentials
    pub fn to_sync_endpoint(&self) -> odf::metadata::tasks::SyncEndpoint {
        odf::metadata::tasks::SyncEndpoint {
            url: self.url.to_string(),
            auth: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum FindPullAliasError {
    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveRemoteSourceError {
    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<ResolveAliasError> for ResolveRemoteSourceError {
    fn from(value: ResolveAliasError) -> Self {
        match value {
            ResolveAliasError::RepositoryNotFound(e) => Self::RepositoryNotFound(e),
            e @ (ResolveAliasError::AmbiguousRepository
            | ResolveAliasError::AmbiguousAlias
            | ResolveAliasError::EmptyRepositoryList) => Self::Internal(e.int_err()),
            ResolveAliasError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<ResolveRemoteSourceError> for SyncError {
    fn from(value: ResolveRemoteSourceError) -> Self {
        match value {
            ResolveRemoteSourceError::RepositoryNotFound(e) => Self::RepositoryNotFound(e),
            ResolveRemoteSourceError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolvePullSourceError {
    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,

    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<FindPullAliasError> for ResolvePullSourceError {
    fn from(value: FindPullAliasError) -> Self {
        match value {
            FindPullAliasError::AmbiguousSource => Self::AmbiguousSource,
            FindPullAliasError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<ResolveRemoteSourceError> for ResolvePullSourceError {
    fn from(value: ResolveRemoteSourceError) -> Self {
        match value {
            ResolveRemoteSourceError::RepositoryNotFound(e) => Self::RepositoryNotFound(e),
            ResolveRemoteSourceError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
