use super::{RemoteError, RemoteID, RemoteIDBuf};
use opendatafabric::{DatasetID, DatasetIDBuf, Sha3_256};

use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait SyncService: Send + Sync {
    fn sync_from(
        &self,
        local_dataset_id: &DatasetID,
        remote_dataset_id: &DatasetID,
        remote_id: &RemoteID,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_to(
        &self,
        local_dataset_id: &DatasetID,
        remote_dataset_id: &DatasetID,
        remote_id: &RemoteID,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError>;
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
    },
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

pub trait SyncListener: Send {
    fn begin(&mut self) {}
    fn success(&mut self, _result: &SyncResult) {}
    fn error(&mut self, _error: &SyncError) {}
}

pub struct NullSyncListener;
impl SyncListener for NullSyncListener {}

pub trait SyncMultiListener {
    fn begin_sync(&mut self, _dataset_id: &DatasetID) -> Option<Arc<Mutex<dyn SyncListener>>> {
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
    #[error("Dataset {dataset_id} does not exist in remote {remote_id}")]
    RemoteDatasetDoesNotExist {
        remote_id: RemoteIDBuf,
        dataset_id: DatasetIDBuf,
    },
    #[error("Remote {remote_id} does not exist")]
    RemoteDoesNotExist { remote_id: RemoteIDBuf },
    #[error("Local dataset ({local_head}) and remote ({remote_head}) have diverged")]
    DatasetsDiverged {
        local_head: Sha3_256,
        remote_head: Sha3_256,
    },
    #[error("Remote appears to have corrupted data: {message}")]
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

impl From<RemoteError> for SyncError {
    fn from(e: RemoteError) -> Self {
        match e {
            RemoteError::Diverged {
                remote_head,
                local_head,
            } => SyncError::DatasetsDiverged {
                remote_head: remote_head,
                local_head: local_head,
            },
            RemoteError::Corrupted {
                ref message,
                source: _,
            } => SyncError::Corrupted {
                message: message.clone(),
                source: Box::new(e),
            },
            RemoteError::CredentialsError { .. } => SyncError::CredentialsError(Box::new(e)),
            RemoteError::ProtocolError { .. } => SyncError::CredentialsError(Box::new(e)),
            RemoteError::DoesNotExist => SyncError::RemoteDatasetDoesNotExist {
                remote_id: "unknown".to_owned(),
                dataset_id: DatasetIDBuf::new_unchecked("unknown"),
            },
            RemoteError::UpdatedConcurrently => SyncError::UpdatedConcurrently(Box::new(e)),
            RemoteError::IOError { .. } => SyncError::IOError(Box::new(e)),
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
