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

use dill::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

pub struct TaskSystemEventStoreInMemory {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    events: Vec<TaskSystemEvent>,
    tasks_by_dataset: HashMap<DatasetID, Vec<TaskID>>,
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

#[component(pub)]
#[scope(Singleton)]
impl TaskSystemEventStoreInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn update_index_by_dataset(
        tasks_by_dataset: &mut HashMap<DatasetID, Vec<TaskID>>,
        event: &TaskSystemEvent,
    ) {
        if let TaskSystemEvent::TaskCreated(e) = &event {
            if let Some(dataset_id) = e.logical_plan.dataset_id() {
                let entries = match tasks_by_dataset.entry(dataset_id.clone()) {
                    Entry::Occupied(v) => v.into_mut(),
                    Entry::Vacant(v) => v.insert(Vec::default()),
                };
                entries.push(event.task_id())
            }
        }
    }
}

#[async_trait::async_trait]
impl EventStore<TaskState> for TaskSystemEventStoreInMemory {
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events.len())
    }

    fn get_events<'a>(
        &'a self,
        task_id: &TaskID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, TaskSystemEvent> {
        let task_id = task_id.clone();

        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut seen = opts.from.map(|id| (id.into_inner() + 1) as usize).unwrap_or(0);

            loop {
                let next = {
                    let s = self.state.lock().unwrap();

                    let to = opts.to.map(|id| (id.into_inner() + 1) as usize).unwrap_or(s.events.len());

                    s.events[..to]
                        .iter()
                        .enumerate()
                        .skip(seen)
                        .filter(|(_, e)| e.task_id() == task_id)
                        .map(|(i, e)| (i, e.clone()))
                        .next()
                };

                match next {
                    None => break,
                    Some((i, event)) => {
                        seen = i + 1;
                        yield (EventID::new(i as u64), event)
                    }
                }
            }
        })
    }

    // TODO: concurrency
    async fn save_events(
        &self,
        _task_id: &TaskID,
        events: Vec<TaskSystemEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let mut s = self.state.lock().unwrap();

        for event in events {
            Self::update_index_by_dataset(&mut s.tasks_by_dataset, &event);
            s.events.push(event);
        }

        Ok(EventID::new((s.events.len() - 1) as u64))
    }
}

#[async_trait::async_trait]
impl TaskSystemEventStore for TaskSystemEventStoreInMemory {
    fn new_task_id(&self) -> TaskID {
        self.state.lock().unwrap().next_task_id()
    }

    fn get_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> TaskIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let mut pos = {
                let s = self.state.lock().unwrap();
                s.tasks_by_dataset.get(&dataset_id).map(|tasks| tasks.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let s = self.state.lock().unwrap();
                    s.tasks_by_dataset
                        .get(&dataset_id)
                        .and_then(|tasks| tasks.get(pos).cloned())
                };

                let task_id = match next {
                    None => break,
                    Some(task_id) => task_id,
                };

                yield task_id;
            }
        })
    }
}
