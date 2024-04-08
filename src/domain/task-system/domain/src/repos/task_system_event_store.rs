// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskSystemEventStore: EventStore<TaskState> {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError>;

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    async fn get_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
        pagination: TaskPaginationOpts,
    ) -> TaskIDStream;

    /// Returns total number of tasks associated  with the specified dataset
    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<usize, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type TaskIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TaskID, InternalError>> + Send + 'a>>;

#[derive(Debug)]
pub struct TaskPaginationOpts {
    pub offset: usize,
    pub limit: usize,
}

/////////////////////////////////////////////////////////////////////////////////////////
