// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

pub struct TaskEventStoreInMemory {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    events: Vec<TaskEventInstance>,
    tasks_by_dataset: HashMap<DatasetID, Vec<TaskID>>,

    // Auto increment
    last_task_id: Option<TaskID>,
    last_event_id: Option<TaskEventID>,
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

    fn next_event_id(&mut self) -> TaskEventID {
        let new_event_id = if let Some(last_event_id) = self.last_event_id {
            let id: u64 = last_event_id.into();
            TaskEventID::new(id + 1)
        } else {
            TaskEventID::new(0)
        };
        self.last_event_id = Some(new_event_id);
        new_event_id
    }
}

#[component(pub)]
#[scope(Singleton)]
impl TaskEventStoreInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

#[async_trait::async_trait]
impl EventStore for TaskEventStoreInMemory {
    type Agg = Task;
    type EventInstance = TaskEventInstance;

    async fn load(&self, id: &TaskID) -> Result<Task, LoadError<Task>> {
        let event_stream = self.get_events_by_task(id, None, None);
        match Task::from_event_stream(event_stream).await? {
            Some(agg) => Ok(agg),
            None => Err(AggrateNotFoundError::new(*id).into()),
        }
    }

    async fn save_event(&self, event: TaskEvent) -> Result<(), SaveError> {
        let mut s = self.state.lock().unwrap();

        if let TaskEvent::Created(e) = &event {
            if let Some(dataset_id) = e.logical_plan.dataset_id() {
                let entries = match s.tasks_by_dataset.entry(dataset_id.clone()) {
                    Entry::Occupied(v) => v.into_mut(),
                    Entry::Vacant(v) => v.insert(Vec::default()),
                };
                entries.push(event.task_id())
            }
        }

        let event_id = s.next_event_id();
        s.events.push(TaskEventInstance {
            event_id,
            event_time: Utc::now(),
            event,
        });
        Ok(())
    }

    async fn save_events(&self, events: Vec<TaskEvent>) -> Result<(), SaveError> {
        let mut s = self.state.lock().unwrap();

        let event_time = Utc::now();

        for event in events {
            let event_id = s.next_event_id();
            s.events.push(TaskEventInstance {
                event_id,
                event_time: event_time.clone(),
                event,
            });
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl TaskEventStore for TaskEventStoreInMemory {
    fn new_task_id(&self) -> TaskID {
        self.state.lock().unwrap().next_task_id()
    }

    fn get_events_by_task<'a>(
        &'a self,
        task_id: &TaskID,
        as_of_id: Option<&TaskEventID>,
        as_of_time: Option<&DateTime<Utc>>,
    ) -> TaskEventStream<'a> {
        // TODO: Implement
        assert_eq!(as_of_id, None);
        assert_eq!(as_of_time, None);

        let task_id = task_id.clone();

        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut current = 0;

            loop {
                let next = {
                    let s = self.state.lock().unwrap();
                    s.events[current..]
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| e.event.task_id() == task_id)
                        .map(|(i, e)| (current + i, e.clone()))
                        .next()
                };

                let event = match next {
                    None => break,
                    Some((i, event)) => {
                        current = i + 1;
                        event
                    }
                };

                yield event;
            }
        })
    }

    fn get_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> TaskIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let mut i = 0;

            loop {
                let next = {
                    let s = self.state.lock().unwrap();
                    s.tasks_by_dataset
                        .get(&dataset_id)
                        .and_then(|tasks| tasks.get(i).cloned())
                };

                let task_id = match next {
                    None => break,
                    Some(task_id) => task_id,
                };

                i += 1;
                yield task_id;
            }
        })
    }
}
