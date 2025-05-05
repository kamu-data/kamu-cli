// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::{Entry, HashMap};
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use dill::*;
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryTaskEventStore {
    inner: InMemoryEventStore<TaskState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<TaskEvent>,
    tasks_by_dataset: HashMap<odf::DatasetID, Vec<TaskID>>,
    task_data: BTreeMap<TaskID, (TaskStatus, Option<DateTime<Utc>>)>,
    last_task_id: Option<TaskID>,
}

impl State {
    fn next_task_id(&mut self) -> TaskID {
        let new_task_id = if let Some(last_task_id) = self.last_task_id {
            let id: u64 = last_task_id.into();
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskEventStore)]
#[scope(Singleton)]
impl InMemoryTaskEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }

    fn update_index(state: &mut State, event: &TaskEvent) {
        if let TaskEvent::TaskCreated(e) = &event {
            if let Some(dataset_id) = e.logical_plan.dataset_id() {
                let entries = match state.tasks_by_dataset.entry(dataset_id.clone()) {
                    Entry::Occupied(v) => v.into_mut(),
                    Entry::Vacant(v) => v.insert(Vec::default()),
                };
                entries.push(event.task_id());
            }
        }

        state.task_data.insert(
            event.task_id(),
            (event.new_status(), event.next_attempt_at()),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<TaskState> for InMemoryTaskEventStore {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<TaskEvent> {
        self.inner.get_events(task_id, opts)
    }

    async fn save_events(
        &self,
        task_id: &TaskID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index(&mut g, event);
            }
        }

        self.inner
            .save_events(task_id, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskEventStore for InMemoryTaskEventStore {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        Ok(self.inner.as_state().lock().unwrap().next_task_id())
    }

    async fn try_get_queued_task(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<TaskID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let maybe_task_id = g
            .task_data
            .iter()
            .filter_map(|(id, (status, next_attempt_at))| match status {
                TaskStatus::Queued | TaskStatus::Retrying => {
                    assert!(next_attempt_at.is_some());
                    if next_attempt_at.unwrap() > now {
                        None
                    } else {
                        Some((id, next_attempt_at.unwrap()))
                    }
                }
                TaskStatus::Running | TaskStatus::Finished => {
                    assert!(next_attempt_at.is_none());
                    None
                }
            })
            .inspect(|(id, time)| {
                println!(
                    "Task ID: {}, Next Attempt At: {}",
                    id,
                    time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                );
            })
            .min_by(|(id_a, time_a), (id_b, time_b)| match time_a.cmp(time_b) {
                std::cmp::Ordering::Equal => id_a.cmp(id_b),
                ord => ord,
            })
            .map(|(id, _)| *id);

        Ok(maybe_task_id)
    }

    /// Returns list of tasks, which are in Running state,
    /// from earliest to latest
    fn get_running_tasks(&self, pagination: PaginationOpts) -> TaskIDStream {
        let task_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.task_data
                .iter()
                .filter(|(_, (status, _))| *status == TaskStatus::Running)
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|(id, _)| Ok(*id))
                .collect()
        };

        Box::pin(futures::stream::iter(task_ids_page))
    }

    /// Returns total number of tasks, which are in Running state
    async fn get_count_running_tasks(&self) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        let mut count = 0;

        for (task_status, _) in g.task_data.values() {
            if *task_status == TaskStatus::Running {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    /// Note: no longer used, but might be used in future (admin view)
    fn get_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        pagination: PaginationOpts,
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

    /// Returns total number of tasks associated  with the specified dataset
    /// Note: no longer used, but might be used in future (admin view)
    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.tasks_by_dataset.get(dataset_id).map_or(0, Vec::len))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
