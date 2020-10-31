use opendatafabric::DatasetSnapshot;

use std::backtrace::Backtrace;
use thiserror::Error;

pub trait ResourceLoader {
    fn load_dataset_snapshot_from_path(
        &self,
        path: &std::path::Path,
    ) -> Result<DatasetSnapshot, ResourceError>;

    fn load_dataset_snapshot_from_url(
        &self,
        url: &url::Url,
    ) -> Result<DatasetSnapshot, ResourceError>;

    fn load_dataset_snapshot_from_ref(&self, sref: &str) -> Result<DatasetSnapshot, ResourceError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum ResourceError {
    #[error("Source is unreachable at {path}")]
    Unreachable {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },
    #[error("Source not found at {path}")]
    NotFound {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },
    #[error("Serde error: {source}")]
    SerdeError {
        #[source]
        source: BoxedError,
        backtrace: Backtrace,
    },
    #[error("Internal error: {source}")]
    InternalError {
        #[from]
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl ResourceError {
    pub fn unreachable(path: String, source: Option<BoxedError>) -> Self {
        Self::Unreachable {
            path: path,
            source: source,
        }
    }

    pub fn not_found(path: String, source: Option<BoxedError>) -> Self {
        Self::NotFound {
            path: path,
            source: source,
        }
    }

    pub fn serde(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::SerdeError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
