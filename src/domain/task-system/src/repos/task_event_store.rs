// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskEventStore: Sync + Send {
    /// Generates new unique task identifier
    fn new_task_id(&self) -> TaskID;

    // TODO: Concurrency control
    /// Persists a singular event
    async fn save_event(&self, event: TaskEvent) -> Result<TaskEventID, InternalError>;

    // TODO: Concurrency control
    /// Persists a series of events
    async fn save_events(&self, events: Vec<TaskEvent>) -> Result<TaskEventID, InternalError>;

    /// Returns the event history of the specified task in chronological order
    fn get_events_by_task<'a>(
        &'a self,
        task_id: &TaskID,
        as_of_id: Option<&TaskEventID>,
        as_of_time: Option<&DateTime<Utc>>,
    ) -> TaskEventStream<'a>;

    /// Returns the tasks associated with the specified dataset in reverse
    /// chronological order based on creation time
    fn get_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> TaskIDStream<'a>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type TaskEventStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<TaskEventInstance, InternalError>> + Send + 'a>,
>;

pub type TaskIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TaskID, InternalError>> + Send + 'a>>;
