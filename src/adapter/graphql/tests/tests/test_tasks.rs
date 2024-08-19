// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use chrono::Utc;
use dill::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    TaskScheduler {}
    #[async_trait::async_trait]
    impl TaskScheduler for TaskScheduler {
        async fn get_task(&self, task_id: TaskID) -> Result<TaskState, GetTaskError>;
        async fn list_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID, pagination: TaskPaginationOpts) -> Result<TaskStateListing<'a>, ListTasksByDatasetError>;
        async fn create_task(&self, plan: LogicalPlan) -> Result<TaskState, CreateTaskError>;
        async fn cancel_task(&self, task_id: TaskID) -> Result<TaskState, CancelTaskError>;
        async fn take(&self) -> Result<TaskID, TakeTaskError>;
        async fn try_take(&self) -> Result<Option<TaskID>, TakeTaskError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_get_non_existing() {
    let mut task_sched_mock = MockTaskScheduler::new();
    task_sched_mock.expect_get_task().return_once(|_| {
        Err(GetTaskError::NotFound(TaskNotFoundError {
            task_id: TaskID::new(1),
        }))
    });

    let cat = dill::CatalogBuilder::new()
        .add_value(task_sched_mock)
        .bind::<dyn TaskScheduler, MockTaskScheduler>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(
                r#"{
                    tasks {
                        getTask (taskId: "123") {
                            taskId
                        }
                    }
                }"#,
            )
            .data(cat),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "getTask": null,
            }
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_task_get_existing() {
    let returned_task = TaskState {
        task_id: TaskID::new(123),
        status: TaskStatus::Finished(TaskOutcome::Success(TaskResult::Empty)),
        cancellation_requested: false,
        logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: DatasetID::new_seeded_ed25519(b"foo"),
            fetch_uncacheable: false,
        }),
        created_at: Utc::now(),
        ran_at: None,
        cancellation_requested_at: None,
        finished_at: None,
    };
    let expected_task = returned_task.clone();

    let mut task_sched_mock = MockTaskScheduler::new();
    task_sched_mock
        .expect_get_task()
        .with(mockall::predicate::eq(expected_task.task_id))
        .return_once(move |_| Ok(returned_task));

    let cat = dill::CatalogBuilder::new()
        .add_value(task_sched_mock)
        .bind::<dyn TaskScheduler, MockTaskScheduler>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"{{
                    tasks {{
                        getTask (taskId: "{}") {{
                            taskId
                            status
                            cancellationRequested
                            outcome
                        }}
                    }}
                }}"#,
                expected_task.task_id,
            ))
            .data(cat),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "getTask": {
                    "taskId": expected_task.task_id.to_string(),
                    "cancellationRequested": false,
                    "status": "FINISHED",
                    "outcome": "SUCCESS",
                },
            }
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_task_list_by_dataset() {
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

    let returned_task = TaskState {
        task_id: TaskID::new(123),
        status: TaskStatus::Queued,
        cancellation_requested: false,
        logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: dataset_id.clone(),
            fetch_uncacheable: false,
        }),
        created_at: Utc::now(),
        ran_at: None,
        cancellation_requested_at: None,
        finished_at: None,
    };
    let expected_task = returned_task.clone();

    let mut task_sched_mock = MockTaskScheduler::new();
    task_sched_mock
        .expect_list_tasks_by_dataset()
        .return_once(move |_, _| {
            Ok(TaskStateListing {
                stream: Box::pin(futures::stream::once(async { Ok(returned_task) })),
                total_count: 1,
            })
        });

    let cat = dill::CatalogBuilder::new()
        .add_value(task_sched_mock)
        .bind::<dyn TaskScheduler, MockTaskScheduler>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"{{
                    tasks {{
                        listTasksByDataset (datasetId: "{dataset_id}") {{
                            nodes {{
                                taskId
                                status
                                outcome
                            }}
                            pageInfo {{
                                hasPreviousPage
                                hasNextPage
                                currentPage
                                totalPages
                            }}
                        }}
                    }}
                }}"#,
            ))
            .data(cat),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "listTasksByDataset": {
                    "nodes": [{
                        "taskId": expected_task.task_id.to_string(),
                        "status": "QUEUED",
                        "outcome": null,
                    }],
                    "pageInfo": {
                        "hasPreviousPage": false,
                        "hasNextPage": false,
                        "currentPage": 0,
                        "totalPages": 1,
                    },
                },
            },
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_task_create_probe() {
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

    let expected_logical_plan = LogicalPlan::Probe(Probe {
        dataset_id: Some(dataset_id.clone()),
        busy_time: Some(std::time::Duration::from_millis(500)),
        end_with_outcome: Some(TaskOutcome::Failed(TaskError::Empty)),
    });
    let returned_task = TaskState {
        task_id: TaskID::new(123),
        status: TaskStatus::Queued,
        cancellation_requested: false,
        logical_plan: expected_logical_plan.clone(),
        created_at: Utc::now(),
        ran_at: None,
        cancellation_requested_at: None,
        finished_at: None,
    };
    let expected_task = returned_task.clone();

    let mut task_sched_mock = MockTaskScheduler::new();
    task_sched_mock
        .expect_create_task()
        .withf(move |logical_plan| *logical_plan == expected_logical_plan)
        .return_once(move |_| Ok(returned_task));

    let base_cat = create_catalog(task_sched_mock);
    let (cat_anonymous, cat_authorized) = authentication_catalogs(&base_cat).await;

    let request_code = format!(
        r#"mutation {{
                tasks {{
                    createProbeTask (datasetId: "{dataset_id}", busyTimeMs: 500, endWithOutcome: FAILED) {{
                        taskId
                    }}
                }}
            }}"#
    );

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(cat_anonymous))
        .await;
    expect_anonymous_access_error(res);

    let res = schema
        .execute(async_graphql::Request::new(request_code).data(cat_authorized))
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "createProbeTask": {
                    "taskId": expected_task.task_id.to_string(),
                },
            },
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog(task_sched_mock: MockTaskScheduler) -> Catalog {
    let mut b = CatalogBuilder::new();

    b.add_value(task_sched_mock)
        .bind::<dyn TaskScheduler, MockTaskScheduler>();

    database_common::NoOpDatabasePlugin::init_database_components(&mut b);

    b.build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
