// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use dill::*;
use kamu_core::{PullOptions, PullService};
use kamu_task_system::*;
use opendatafabric::DatasetID;

pub struct TaskServiceInMemory {
    pull_svc: Arc<dyn PullService>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    next_free_task_id: u64,
    task_queue: VecDeque<TaskID>,
    task_state: HashMap<TaskID, TaskState>,
    task_loop_hdl: Option<tokio::task::JoinHandle<()>>,
}

#[component(pub)]
#[scope(Singleton)]
impl TaskServiceInMemory {
    pub fn new(pull_svc: Arc<dyn PullService>) -> Self {
        Self {
            pull_svc,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    // TODO: Panic tapping?
    async fn run_tasks_loop(state: Arc<Mutex<State>>, pull_svc: Arc<dyn PullService>) {
        loop {
            // Try to steal a task from the queue
            let task = {
                let mut s = state.lock().unwrap();
                if let Some(task_id) = s.task_queue.pop_front() {
                    let task_state = s.task_state.get_mut(&task_id).unwrap();
                    task_state.status = TaskStatus::Running;
                    Some(task_state.clone())
                } else {
                    None
                }
            };

            let task = match task {
                Some(t) => t,
                None => {
                    // TODO: Use signaling to wake only when needed
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            tracing::info!(
                task_id = %task.task_id,
                logical_plan = ?task.logical_plan,
                "Executing task",
            );

            let outcome = match &task.logical_plan {
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
                task_id = %task.task_id,
                logical_plan = ?task.logical_plan,
                ?outcome,
                "Task finished",
            );

            {
                let mut s = state.lock().unwrap();
                let task_state = s.task_state.get_mut(&task.task_id).unwrap();
                task_state.status = TaskStatus::Finished(outcome);
            }
        }
    }
}

#[async_trait::async_trait]
impl TaskService for TaskServiceInMemory {
    #[tracing::instrument(level = "info", skip_all, fields(?logical_plan))]
    async fn create_task(&self, logical_plan: LogicalPlan) -> Result<TaskState, CreateTaskError> {
        let (task, queue_len) = {
            let mut state = self.state.lock().unwrap();

            let task_id = TaskID::new(state.next_free_task_id);
            state.next_free_task_id += 1;

            let task = TaskState {
                task_id,
                status: TaskStatus::Queued,
                logical_plan,
            };

            state.task_state.insert(task_id, task.clone());
            state.task_queue.push_back(task_id);

            // Create loop task upon first run
            if state.task_loop_hdl.is_none() {
                state.task_loop_hdl = Some(tokio::spawn(Self::run_tasks_loop(
                    self.state.clone(),
                    self.pull_svc.clone(),
                )));
            }

            (task, state.task_queue.len())
        }; // lock

        tracing::info!(
            task_id = %task.task_id,
            queue_len,
            "Created new task"
        );

        Ok(task)
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn get_task(&self, task_id: &TaskID) -> Result<TaskState, GetTaskError> {
        let state = self.state.lock().unwrap();
        let Some(task) = state.task_state.get(task_id) else {
            return Err(TaskNotFoundError { task_id: *task_id }.into())
        };
        Ok(task.clone())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    fn list_tasks_by_dataset(&self, dataset_id: &DatasetID) -> TaskStateStream {
        let dataset_id = dataset_id.clone();
        let state = self.state.clone();

        Box::pin(async_stream::try_stream! {
            let mut position = 0;
            loop {
                let maybe_task = {
                    let state = state.lock().unwrap();
                    // TODO: This is horrible in every single way.
                    // Real implementation should be reconstituting from event history.
                    // That's how we'll get correct ordering too.
                    state.task_state
                        .values()
                        .filter(|t| match &t.logical_plan {
                            LogicalPlan::UpdateDataset(upd) => upd.dataset_id == dataset_id,
                            LogicalPlan::Probe(p) => p.dataset_id.as_ref() == Some(&dataset_id),
                        })
                        .skip(position)
                        .next()
                        .map(|t| t.clone())
                };

                let Some(task) = maybe_task else {
                    break
                };

                position += 1;
                yield task;
            }
        })
    }
}
