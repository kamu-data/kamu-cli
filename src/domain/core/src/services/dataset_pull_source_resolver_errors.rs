// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{GetRepoError, RepositoryNotFoundError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset has several pull aliases, so its pull source cannot be chosen
#[derive(Debug, Error)]
#[error("Cannot choose between multiple pull aliases")]
pub struct AmbiguousPullSourceError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum FindPullAliasError {
    #[error(transparent)]
    AmbiguousSource(#[from] AmbiguousPullSourceError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A remote dataset named only by its ID: a repository cannot be searched
/// for it, so there is no URL to sync from
#[derive(Debug, Error)]
#[error("Syncing remote dataset by ID is not yet supported: {remote_ref}")]
pub struct RemoteDatasetByIdUnsupportedError {
    pub remote_ref: odf::DatasetRefRemote,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveRemoteSourceError {
    #[error(transparent)]
    ByIdUnsupported(#[from] RemoteDatasetByIdUnsupportedError),

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

impl From<GetRepoError> for ResolveRemoteSourceError {
    fn from(value: GetRepoError) -> Self {
        match value {
            GetRepoError::NotFound(e) => Self::RepositoryNotFound(e),
            GetRepoError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolvePullSourceError {
    #[error(transparent)]
    AmbiguousSource(#[from] AmbiguousPullSourceError),

    #[error(transparent)]
    ByIdUnsupported(#[from] RemoteDatasetByIdUnsupportedError),

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
            FindPullAliasError::AmbiguousSource(e) => Self::AmbiguousSource(e),
            FindPullAliasError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<ResolveRemoteSourceError> for ResolvePullSourceError {
    fn from(value: ResolveRemoteSourceError) -> Self {
        match value {
            ResolveRemoteSourceError::ByIdUnsupported(e) => Self::ByIdUnsupported(e),
            ResolveRemoteSourceError::RepositoryNotFound(e) => Self::RepositoryNotFound(e),
            ResolveRemoteSourceError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
