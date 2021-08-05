use super::RemoteError;
use opendatafabric::{DatasetID, DatasetIDBuf, DatasetRef, DatasetRefBuf, RemoteIDBuf, Sha3_256};

use std::sync::{Arc, Mutex};
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
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetRef, &DatasetID)>,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetRefBuf, DatasetIDBuf), Result<SyncResult, SyncError>)>;

    fn sync_to(
        &self,
        local_id: &DatasetID,
        remote_ref: &DatasetRef,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetID, &DatasetRef)>,
        options: SyncOptions,
        listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetIDBuf, DatasetRefBuf), Result<SyncResult, SyncError>)>;
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
    fn begin(&mut self) {}
    fn success(&mut self, _result: &SyncResult) {}
    fn error(&mut self, _error: &SyncError) {}
}

pub struct NullSyncListener;
impl SyncListener for NullSyncListener {}

pub trait SyncMultiListener {
    fn begin_sync(
        &mut self,
        _local_id: &DatasetID,
        _remote_ref: &DatasetRef,
    ) -> Option<Arc<Mutex<dyn SyncListener>>> {
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
    #[error("Dataset {dataset_ref} does not exist in remote")]
    RemoteDatasetDoesNotExist { dataset_ref: DatasetRefBuf },
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
                dataset_ref: DatasetRefBuf::new_unchecked("unknown/unknown"),
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
