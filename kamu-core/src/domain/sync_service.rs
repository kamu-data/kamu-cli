use super::RepositoryError;
use opendatafabric::{DatasetID, DatasetIDBuf, DatasetRef, DatasetRefBuf, RepositoryBuf, Sha3_256};

use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait SyncService: Send + Sync {
    fn sync_from(
        &self,
        remote_ref: &DatasetRef,
        local_id: &DatasetID,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetRef, &DatasetID)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<((DatasetRefBuf, DatasetIDBuf), Result<SyncResult, SyncError>)>;

    fn sync_to(
        &self,
        local_id: &DatasetID,
        remote_ref: &DatasetRef,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetID, &DatasetRef)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<((DatasetIDBuf, DatasetRefBuf), Result<SyncResult, SyncError>)>;

    /// Deletes a dataset from a remote repository.
    ///
    /// Note: Some repos may not permit this operation.
    fn delete(&self, remote_ref: &DatasetRef) -> Result<(), SyncError>;
}

#[derive(Debug, Clone)]
pub struct SyncOptions {}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub enum SyncResult {
    UpToDate,
    Updated {
        old_head: Option<Sha3_256>,
        new_head: Sha3_256,
        num_blocks: usize,
    },
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

pub trait SyncListener: Send {
    fn begin(&self) {}
    fn success(&self, _result: &SyncResult) {}
    fn error(&self, _error: &SyncError) {}
}

pub struct NullSyncListener;
impl SyncListener for NullSyncListener {}

pub trait SyncMultiListener {
    fn begin_sync(
        &self,
        _local_id: &DatasetID,
        _remote_ref: &DatasetRef,
    ) -> Option<Arc<dyn SyncListener>> {
        None
    }
}

pub struct NullSyncMultiListener;
impl SyncMultiListener for NullSyncMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Dataset {dataset_id} does not exist locally")]
    LocalDatasetDoesNotExist { dataset_id: DatasetIDBuf },
    #[error("Dataset {dataset_ref} does not exist in repository")]
    RemoteDatasetDoesNotExist { dataset_ref: DatasetRefBuf },
    #[error("Repository {repo_id} does not exist")]
    RepositoryDoesNotExist { repo_id: RepositoryBuf },
    // TODO: Report divergence type (e.g. remote is ahead of local)
    //#[error("Local dataset ({local_head}) and remote ({remote_head}) have diverged (remote is ahead by {uncommon_blocks_in_remote} blocks, local is ahead by {uncommon_blocks_in_local})")]
    #[error("Local dataset ({local_head}) and remote ({remote_head}) have diverged")]
    DatasetsDiverged {
        local_head: Sha3_256,
        remote_head: Sha3_256,
        //uncommon_blocks_in_local: usize,
        //uncommon_blocks_in_remote: usize,
    },
    #[error("Repository does not allow this operation")]
    NotAllowed,
    #[error("Repository appears to have corrupted data: {message}")]
    Corrupted {
        message: String,
        #[source]
        source: BoxedError,
    },
    #[error("{0}")]
    UpdatedConcurrently(#[source] BoxedError),
    #[error("{0}")]
    IOError(#[source] BoxedError),
    #[error("{0}")]
    CredentialsError(#[source] BoxedError),
    #[error("{0}")]
    ProtocolError(#[source] BoxedError),
    #[error("{0}")]
    InternalError(#[source] BoxedError),
}

impl From<RepositoryError> for SyncError {
    fn from(e: RepositoryError) -> Self {
        match e {
            RepositoryError::NotAllowed => SyncError::NotAllowed,
            RepositoryError::Diverged {
                local_head,
                remote_head,
            } => SyncError::DatasetsDiverged {
                local_head,
                remote_head,
            },
            RepositoryError::Corrupted {
                ref message,
                source: _,
            } => SyncError::Corrupted {
                message: message.clone(),
                source: Box::new(e),
            },
            RepositoryError::CredentialsError { .. } => SyncError::CredentialsError(Box::new(e)),
            RepositoryError::ProtocolError { .. } => SyncError::ProtocolError(Box::new(e)),
            RepositoryError::DoesNotExist => SyncError::RemoteDatasetDoesNotExist {
                dataset_ref: DatasetRefBuf::new_unchecked("unknown/unknown"),
            },
            RepositoryError::UpdatedConcurrently => SyncError::UpdatedConcurrently(Box::new(e)),
            RepositoryError::IOError { .. } => SyncError::IOError(Box::new(e)),
        }
    }
}

impl From<std::io::Error> for SyncError {
    fn from(e: std::io::Error) -> Self {
        Self::InternalError(e.into())
    }
}

impl From<fs_extra::error::Error> for SyncError {
    fn from(e: fs_extra::error::Error) -> Self {
        Self::InternalError(e.into())
    }
}
