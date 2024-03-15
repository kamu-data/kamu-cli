// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::{Entry, HashMap};

use dill::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskSystemEventStoreInMemory {
    inner: EventStoreInMemory<TaskState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<TaskEvent>,
    tasks_by_dataset: HashMap<DatasetID, Vec<TaskID>>,
    last_task_id: Option<TaskID>,
}

impl State {
    fn next_task_id(&mut self) -> TaskID {
        let new_task_id = if let Some(last_task_id) = self.last_task_id {
            let id: i64 = last_task_id.into();
            TaskID::new(id + 1)
        } else {
            TaskID::new(0)
        };
        self.last_task_id = Some(new_task_id);
        new_task_id
    }
}

impl EventStoreState<TaskState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[<TaskState as Projection>::Event] {
        &self.events
    }

    fn add_event(&mut self, event: <TaskState as Projection>::Event) {
        self.events.push(event);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskSystemEventStore)]
#[scope(Singleton)]
impl TaskSystemEventStoreInMemory {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }

    fn update_index_by_dataset(
        tasks_by_dataset: &mut HashMap<DatasetID, Vec<TaskID>>,
        event: &TaskEvent,
    ) {
        if let TaskEvent::TaskCreated(e) = &event {
            if let Some(dataset_id) = e.logical_plan.dataset_id() {
                let entries = match tasks_by_dataset.entry(dataset_id.clone()) {
                    Entry::Occupied(v) => v.into_mut(),
                    Entry::Vacant(v) => v.insert(Vec::default()),
                };
                entries.push(event.task_id());
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<TaskState> for TaskSystemEventStoreInMemory {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    async fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<TaskEvent> {
        self.inner.get_events(task_id, opts).await
    }

    async fn save_events(
        &self,
        task_id: &TaskID,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index_by_dataset(&mut g.tasks_by_dataset, event);
            }
        }

        self.inner.save_events(task_id, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskSystemEventStore for TaskSystemEventStoreInMemory {
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        Ok(self.inner.as_state().lock().unwrap().next_task_id())
    }

    async fn get_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
        pagination: TaskPaginationOpts,
    ) -> TaskIDStream {
        let task_ids_page: Option<Vec<_>> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.tasks_by_dataset.get(dataset_id).map(|dataset_task_ids| {
                dataset_task_ids
                    .iter()
                    .rev()
                    .skip(pagination.offset)
                    .take(pagination.limit)
                    .map(|id| Ok(*id))
                    .collect()
            })
        };

        match task_ids_page {
            Some(task_ids_page) => Box::pin(futures::stream::iter(task_ids_page)),
            None => Box::pin(futures::stream::empty()),
        }
    }

    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.tasks_by_dataset.get(dataset_id).map_or(0, Vec::len))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
