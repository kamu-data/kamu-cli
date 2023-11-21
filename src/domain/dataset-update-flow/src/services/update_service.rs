// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::DatasetNotFoundError;
use kamu_task_system::{TaskID, TaskOutcome};
use opendatafabric::{AccountID, AccountName, DatasetID};
use tokio_stream::Stream;

use crate::{UpdateID, UpdateScheduleState, UpdateState};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateService: Sync + Send {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError>;

    /// Creates a new manual update request
    async fn request_manual_update(
        &self,
        dataset_id: DatasetID,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<UpdateState, RequestUpdateError>;

    /// Returns states of updates associated with a given dataset ordered by
    /// creation time from newest to oldest
    fn list_updates_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<UpdateStateStream, ListUpdatesByDatasetError>;

    /// Returns current state of a given update
    async fn get_update(&self, update_id: UpdateID) -> Result<UpdateState, GetUpdateError>;

    /// Attempts to cancel the given update
    async fn cancel_update(
        &self,
        update_id: UpdateID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<UpdateState, CancelUpdateError>;

    /// Handles task execution outcome.
    /// Reacts correspondingly if the task is related to updates
    ///
    /// TODO: connect to event bus
    async fn on_task_finished(
        &self,
        task_id: TaskID,
        task_outcome: TaskOutcome,
    ) -> Result<Option<UpdateState>, InternalError>;

    /// Notifies about changes in dataset update schedule
    ///
    /// TODO: connect to event bus
    async fn update_schedule_modified(
        &self,
        update_schedule_state: UpdateScheduleState,
    ) -> Result<(), InternalError>;
}
/////////////////////////////////////////////////////////////////////////////////////////

pub type UpdateStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<UpdateState, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RequestUpdateError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListUpdatesByDatasetError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetUpdateError {
    #[error(transparent)]
    NotFound(#[from] UpdateNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum CancelUpdateError {
    #[error(transparent)]
    NotFound(#[from] UpdateNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Update {update_id} not found")]
pub struct UpdateNotFoundError {
    pub update_id: UpdateID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<UpdateState>> for GetUpdateError {
    fn from(value: LoadError<UpdateState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(UpdateNotFoundError {
                update_id: err.query,
            }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<UpdateState>> for CancelUpdateError {
    fn from(value: LoadError<UpdateState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(UpdateNotFoundError {
                update_id: err.query,
            }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
