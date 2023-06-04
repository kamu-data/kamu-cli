// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dill::*;
use kamu_domain_task_system::*;
use opendatafabric::DatasetID;

pub struct TaskServiceInMemory {
    state: Arc<Mutex<State>>,
}

struct State {
    tasks: HashMap<TaskID, TaskState>,
}

#[component(pub)]
#[scope(Singleton)]
impl TaskServiceInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                tasks: HashMap::new(),
            })),
        }
    }
}

#[async_trait::async_trait]
impl TaskService for TaskServiceInMemory {
    #[tracing::instrument(level = "info", skip_all, fields(?logical_plan))]
    async fn create_task(&self, logical_plan: LogicalPlan) -> Result<TaskState, CreateTaskError> {
        let mut state = self.state.lock().unwrap();
        let task_id = TaskID::new(1);
        let task = TaskState {
            task_id,
            status: TaskStatus::Queued,
            logical_plan,
        };
        state.tasks.insert(task_id, task.clone());
        tracing::info!(%task_id, "Created new task");
        Ok(task)
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn get_task(&self, task_id: &TaskID) -> Result<TaskState, GetTaskError> {
        let state = self.state.lock().unwrap();
        let Some(task) = state.tasks.get(task_id) else {
            return Err(TaskNotFoundError { task_id: *task_id }.into())
        };
        Ok(task.clone())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    fn list_tasks_by_dataset(&self, dataset_id: &DatasetID) -> TaskStateStream {
        let state = self.state.clone();
        Box::pin(async_stream::try_stream! {
            let mut position = 0;
            loop {
                let maybe_task = {
                    let state = state.lock().unwrap();
                    // TODO: This is horrible in every single way.
                    // Real implementation should be reconstituting from event history.
                    // That's how we'll get correct ordering too.
                    state.tasks.values().skip(position).next().map(|t| t.clone())
                };

                let Some(task) = maybe_task else {
                    break
                };

                position += 1;
                yield task
            }
        })
    }
}
