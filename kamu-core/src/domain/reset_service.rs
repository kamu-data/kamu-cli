use crate::domain::*;
use opendatafabric::*;
use thiserror::Error;

#[async_trait::async_trait]
pub trait ResetService: Send + Sync {
    async fn reset_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
        block_hash: &Multihash,
    ) -> Result<(), ResetError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResetError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
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

impl From<GetDatasetError> for ResetError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<SetRefError> for ResetError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => Self::Access(e),
            SetRefError::Internal(e) => Self::Internal(e),
        }
    }
}
