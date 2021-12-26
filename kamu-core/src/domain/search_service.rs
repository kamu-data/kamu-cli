// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::RepositoryError;
use opendatafabric::{RemoteDatasetName, RepositoryName};

use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait SearchService: Send + Sync {
    fn search(
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
    #[error("{0}")]
    IOError(#[source] BoxedError),
    #[error("{0}")]
    CredentialsError(#[source] BoxedError),
    #[error("{0}")]
    ProtocolError(#[source] BoxedError),
    #[error("{0}")]
    InternalError(#[source] BoxedError),
}

impl From<RepositoryError> for SearchError {
    fn from(e: RepositoryError) -> Self {
        match e {
            RepositoryError::Corrupted {
                ref message,
                source: _,
            } => SearchError::Corrupted {
                message: message.clone(),
                source: Box::new(e),
            },
            RepositoryError::CredentialsError { .. } => SearchError::CredentialsError(Box::new(e)),
            RepositoryError::ProtocolError { .. } => SearchError::CredentialsError(Box::new(e)),
            RepositoryError::IOError { .. } => SearchError::IOError(Box::new(e)),
            e @ _ => SearchError::InternalError(Box::new(e)),
        }
    }
}

impl From<std::io::Error> for SearchError {
    fn from(e: std::io::Error) -> Self {
        Self::InternalError(e.into())
    }
}
