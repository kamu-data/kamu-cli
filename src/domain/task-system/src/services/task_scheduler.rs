// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetNotFoundError;
use opendatafabric::DatasetID;
use tokio_stream::Stream;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskScheduler: Sync + Send {
    /// Creates a new task from provided logical plan
    async fn create_task(&self, plan: LogicalPlan) -> Result<TaskState, CreateTaskError>;

    /// Returns states of tasks associated with a given dataset ordered by
    /// creation time from newest to oldest
    fn list_tasks_by_dataset(&self, dataset_id: &DatasetID) -> TaskStateStream;

    /// Returns current state of a given task
    async fn get_task(&self, task_id: &TaskID) -> Result<TaskState, GetTaskError>;

    /// Attempts to cancel the given task
    async fn cancel_task(&self, task_id: &TaskID) -> Result<TaskState, CancelTaskError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type TaskStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<TaskState, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CreateTaskError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetTaskError {
    #[error(transparent)]
    NotFound(#[from] TaskNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum CancelTaskError {
    #[error(transparent)]
    NotFound(#[from] TaskNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListTasksByDatasetError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Task {task_id} not found")]
pub struct TaskNotFoundError {
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<Task>> for GetTaskError {
    fn from(value: LoadError<Task>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(TaskNotFoundError { task_id: err.id }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<LoadError<Task>> for CancelTaskError {
    fn from(value: LoadError<Task>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(TaskNotFoundError { task_id: err.id }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}
