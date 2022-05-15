// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{AccessError, InternalError, UnsupportedProtocolError};
use opendatafabric::{RemoteDatasetName, RepositoryName};

use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait SearchService: Send + Sync {
    async fn search(
        &self,
        query: Option<&str>,
        options: SearchOptions,
    ) -> Result<SearchResult, SearchError>;
}

#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub repository_names: Vec<RepositoryName>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchResult {
    // TODO: REMORE ID: Should be RemoteDatasetHandle
    pub datasets: Vec<RemoteDatasetName>,
}

impl SearchResult {
    pub fn merge(mut self, mut other: Self) -> Self {
        self.datasets.append(&mut other.datasets);
        self
    }
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("Repository {repo_name} does not exist")]
    RepositoryDoesNotExist { repo_name: RepositoryName },
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
        UnsupportedProtocolError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}
