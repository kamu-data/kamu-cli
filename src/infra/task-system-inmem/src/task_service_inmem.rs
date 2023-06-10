// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dill::*;
use futures::TryStreamExt;
use kamu_core::{PullOptions, PullService};
use kamu_task_system::*;
use opendatafabric::DatasetID;

pub struct TaskServiceInMemory {
    state: Arc<Mutex<State>>,

    event_store: Arc<dyn TaskEventStore>,
    pull_svc: Arc<dyn PullService>,
}

#[derive(Default)]
struct State {
    task_queue: VecDeque<TaskID>,
    task_loop_hdl: Option<tokio::task::JoinHandle<()>>,
}

#[component(pub)]
#[scope(Singleton)]
impl TaskServiceInMemory {
    pub fn new(event_store: Arc<dyn TaskEventStore>, pull_svc: Arc<dyn PullService>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            event_store,
            pull_svc,
        }
    }

    // TODO: Error handling
    // TODO: Panic tapping?
    async fn run_tasks_loop(
        state: Arc<Mutex<State>>,
        event_store: Arc<dyn TaskEventStore>,
        pull_svc: Arc<dyn PullService>,
    ) {
        loop {
            // Try to steal a task from the queue
            let task_id = {
                let mut s = state.lock().unwrap();
                s.task_queue.pop_front()
            };

            let task_id = match task_id {
                Some(t) => t,
                None => {
                    // TODO: Use signaling to wake only when needed
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            let task_events = event_store.get_events_by_task(&task_id, None, None);
            let task_state = TaskStateProjection::project_stream(task_events.map_ok(|i| i.event))
                .await
                .unwrap()
                .unwrap();

            // TODO: Hide into aggregate
            event_store
                .save_event(TaskRunning { task_id }.into())
                .await
                .unwrap();

            tracing::info!(
                %task_id,
                logical_plan = ?task_state.logical_plan,
                "Executing task",
            );

            let outcome = match &task_state.logical_plan {
                LogicalPlan::UpdateDataset(upd) => {
                    let res = pull_svc
                        .pull(&upd.dataset_id.as_any_ref(), PullOptions::default(), None)
                        .await;

                    match res {
                        Ok(_) => TaskOutcome::Success,
                        Err(_) => TaskOutcome::Failed,
                    }
                }
                LogicalPlan::Probe(Probe {
                    dataset_id: _,
                    busy_time,
                    end_with_outcome,
                }) => {
                    if let Some(busy_time) = busy_time {
                        tokio::time::sleep(busy_time.clone()).await;
                    }
                    end_with_outcome.unwrap_or(TaskOutcome::Success)
                }
            };

            tracing::info!(
                task_id = %task_id,
                logical_plan = ?task_state.logical_plan,
                ?outcome,
                "Task finished",
            );

            event_store
                .save_event(TaskFinished { task_id, outcome }.into())
                .await
                .unwrap();
        }
    }
}

#[async_trait::async_trait]
impl TaskService for TaskServiceInMemory {
    #[tracing::instrument(level = "info", skip_all, fields(?logical_plan))]
    async fn create_task(&self, logical_plan: LogicalPlan) -> Result<TaskState, CreateTaskError> {
        let task_id = self.event_store.new_task_id();
        let event = TaskCreated {
            task_id,
            logical_plan,
        };

        self.event_store.save_event(event.clone().into()).await?;

        let queue_len = {
            let mut state = self.state.lock().unwrap();
            state.task_queue.push_back(task_id);

            // Create loop task upon the first run
            if state.task_loop_hdl.is_none() {
                state.task_loop_hdl = Some(tokio::spawn(Self::run_tasks_loop(
                    self.state.clone(),
                    self.event_store.clone(),
                    self.pull_svc.clone(),
                )));
            }

            state.task_queue.len()
        }; // lock

        tracing::info!(
            %task_id,
            queue_len,
            "Created new task"
        );

        Ok(TaskStateProjection::new(event).into_state())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn get_task(&self, task_id: &TaskID) -> Result<TaskState, GetTaskError> {
        let stream = self.event_store.get_events_by_task(task_id, None, None);

        let task_state = TaskStateProjection::project_stream(stream.map_ok(|i| i.event))
            .await
            .int_err()?;

        task_state.ok_or(TaskNotFoundError { task_id: *task_id }.into())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    fn list_tasks_by_dataset(&self, dataset_id: &DatasetID) -> TaskStateStream {
        let dataset_id = dataset_id.clone();

        // TODO: This requires a lot more thinking on how to make this performant
        Box::pin(async_stream::try_stream! {
            let relevant_tasks: Vec<_> = self
            .event_store
            .get_tasks_by_dataset(&dataset_id)
            .try_collect()
            .await
            .unwrap();

            for task_id in relevant_tasks.into_iter() {
                let events_stream = self.event_store.get_events_by_task(&task_id, None, None);
                let proj = TaskStateProjection::project_stream(events_stream.map_ok(|i| i.event)).await;
                yield proj.int_err()?.unwrap();
            }
        })
    }
}
