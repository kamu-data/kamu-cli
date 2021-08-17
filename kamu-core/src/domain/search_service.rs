use super::RepositoryError;
use opendatafabric::{DatasetRefBuf, RepositoryBuf};

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
    pub repository_ids: Vec<RepositoryBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchResult {
    pub datasets: Vec<DatasetRefBuf>,
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
    #[error("Repository {repo_id} does not exist")]
    RepositoryDoesNotExist { repo_id: RepositoryBuf },
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
