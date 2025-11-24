// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{BoxedError, InternalError};
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides search functionality for datasets that reside in external
/// repositories
#[async_trait::async_trait]
pub trait SearchServiceRemote: Send + Sync {
    async fn search(
        &self,
        query: Option<&str>,
        options: SearchRemoteOpts,
    ) -> Result<SearchRemoteResult, SearchRemoteError>;
}

#[derive(Debug, Clone, Default)]
pub struct SearchRemoteOpts {
    pub repository_names: Vec<odf::RepoName>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchRemoteResult {
    pub datasets: Vec<SearchRemoteResultDataset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRemoteResultDataset {
    pub id: Option<odf::DatasetID>,
    pub alias: odf::DatasetAliasRemote,
    pub kind: Option<odf::DatasetKind>,
    pub num_blocks: Option<u64>,
    pub num_records: Option<u64>,
    pub estimated_size_bytes: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SearchRemoteError {
    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),
    #[error("Repository appears to have corrupted data: {message}")]
    Corrupted {
        message: String,
        #[source]
        source: BoxedError,
    },
    #[error(transparent)]
    UnsupportedProtocol(
        #[from]
        #[backtrace]
        odf::dataset::UnsupportedProtocolError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::metadata::AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetRepoError> for SearchRemoteError {
    fn from(v: GetRepoError) -> Self {
        match v {
            GetRepoError::NotFound(e) => Self::RepositoryNotFound(e),
            GetRepoError::Internal(e) => Self::Internal(e),
        }
    }
}
