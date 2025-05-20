// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::LoadError;
use tokio_stream::Stream;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait TaskScheduler: Sync + Send {
    /// Creates a new task from provided logical plan & metadata
    async fn create_task(
        &self,
        plan: LogicalPlan,
        metadata: Option<TaskMetadata>,
    ) -> Result<TaskState, CreateTaskError>;

    /// Returns current state of a given task
    async fn get_task(&self, task_id: TaskID) -> Result<TaskState, GetTaskError>;

    /// Attempts to cancel the given task
    async fn cancel_task(&self, task_id: TaskID) -> Result<TaskState, CancelTaskError>;

    /// Takes the earliest available queued task, if any, without blocking
    async fn try_take(&self) -> Result<Option<Task>, TakeTaskError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TaskStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<TaskState, InternalError>> + Send + 'a>>;

pub struct TaskStateListing<'a> {
    pub stream: TaskStateStream<'a>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    DatasetNotFound(#[from] odf::DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum TakeTaskError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Task {task_id} not found")]
pub struct TaskNotFoundError {
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<TaskState>> for GetTaskError {
    fn from(value: LoadError<TaskState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(TaskNotFoundError { task_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<LoadError<TaskState>> for CancelTaskError {
    fn from(value: LoadError<TaskState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(TaskNotFoundError { task_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
