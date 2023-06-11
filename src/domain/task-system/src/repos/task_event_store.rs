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
pub trait TaskEventStore: EventStore<Agg = Task> {
    /// Generates new unique task identifier
    fn new_task_id(&self) -> TaskID;

    /// Returns the tasks associated with the specified dataset in reverse
    /// chronological order based on creation time
    fn get_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> TaskIDStream<'a>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type TaskIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TaskID, InternalError>> + Send + 'a>>;
