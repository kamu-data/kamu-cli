// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use event_sourcing::EventStore;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskEventStore: EventStore<TaskState> {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError>;

    /// Attempts to get the earliest queued/retrying task, that is ready to run
    async fn try_get_queued_task(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<TaskID>, InternalError>;

    /// Returns list of tasks, which are in Running state,
    /// from earliest to latest
    fn get_running_tasks(&self, pagination: PaginationOpts) -> TaskIDStream;

    /// Returns total number of tasks, which are in Running state
    async fn get_count_running_tasks(&self) -> Result<usize, InternalError>;

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    /// Note: no longer used, but might be used in future (admin view)
    fn get_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        pagination: PaginationOpts,
    ) -> TaskIDStream;

    /// Returns total number of tasks associated  with the specified dataset
    /// Note: no longer used, but might be used in future (admin view)
    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TaskIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TaskID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
