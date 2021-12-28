// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::RepositoryError;
use opendatafabric::*;

use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait SyncService: Send + Sync {
    fn sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefRemote, DatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefRemote, DatasetName),
        Result<SyncResult, SyncError>,
    )>;

    fn sync_to(
        &self,
        local_ref: &DatasetRefLocal,
        remote_name: &RemoteDatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;

    fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefLocal, RemoteDatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefLocal, RemoteDatasetName),
        Result<SyncResult, SyncError>,
    )>;

    /// Deletes a dataset from a remote repository.
    ///
    /// Note: Some repos may not permit this operation.
    fn delete(&self, remote_ref: &RemoteDatasetName) -> Result<(), SyncError>;
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
        old_head: Option<Multihash>,
        new_head: Multihash,
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
        _local_ref: &DatasetRefLocal,
        _remote_ref: &DatasetRefRemote,
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
    #[error("Dataset {dataset_ref} does not exist locally")]
    LocalDatasetDoesNotExist { dataset_ref: DatasetRefLocal },
    #[error("Dataset {dataset_ref} does not exist in repository")]
    RemoteDatasetDoesNotExist { dataset_ref: DatasetRefRemote },
    #[error("Repository {repo_name} does not exist")]
    RepositoryDoesNotExist { repo_name: RepositoryName },
    // TODO: Report divergence type (e.g. remote is ahead of local)
    //#[error("Local dataset ({local_head}) and remote ({remote_head}) have diverged (remote is ahead by {uncommon_blocks_in_remote} blocks, local is ahead by {uncommon_blocks_in_local})")]
    #[error("Local and remote datasets have diverged. Local head: {local_head}, remote head {remote_head}")]
    DatasetsDiverged {
        local_head: Multihash,
        remote_head: Multihash,
        //uncommon_blocks_in_local: usize,
        //uncommon_blocks_in_remote: usize,
    },
    #[error("Repository does not allow this operation")]
    NotAllowed,
    #[error("Repository appears to have corrupted data: {message}")]
    Corrupted {
        message: String,
        #[source]
        source: Option<BoxedError>,
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
                source: Some(Box::new(e)),
            },
            RepositoryError::CredentialsError { .. } => SyncError::CredentialsError(Box::new(e)),
            RepositoryError::ProtocolError { .. } => SyncError::ProtocolError(Box::new(e)),
            RepositoryError::DoesNotExist => SyncError::RemoteDatasetDoesNotExist {
                dataset_ref: RemoteDatasetName::new_unchecked("unknown/unknown").into(),
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
