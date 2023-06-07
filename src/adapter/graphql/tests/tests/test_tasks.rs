// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

mockall::mock! {
    TaskService {}
    #[async_trait::async_trait]
    impl TaskService for TaskService {
        async fn create_task(&self, plan: LogicalPlan) -> Result<TaskState, CreateTaskError>;
        async fn get_task(&self, task_id: &TaskID) -> Result<TaskState, GetTaskError>;
        fn list_tasks_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> TaskStateStream<'a>;
    }
}

#[test_log::test(tokio::test)]
async fn test_task_get_non_existing() {
    let mut task_svc_mock = MockTaskService::new();
    task_svc_mock.expect_get_task().return_once(|_| {
        Err(GetTaskError::NotFound(TaskNotFoundError {
            task_id: TaskID::new(1),
        }))
    });

    let cat = dill::CatalogBuilder::new()
        .add_value(task_svc_mock)
        .bind::<dyn TaskService, MockTaskService>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(
            r#"{
                tasks {
                    getTask (taskId: "123") {
                        id
                    }
                }
            }"#,
        )
        .await;
    assert!(res.is_ok(), "{:?}", res);
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
        status: TaskStatus::Finished(TaskOutcome::Success),
        logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: DatasetID::from_pub_key_ed25519(b"foo"),
        }),
    };
    let expected_task = returned_task.clone();

    let mut task_svc_mock = MockTaskService::new();
    task_svc_mock
        .expect_get_task()
        .with(mockall::predicate::eq(expected_task.task_id))
        .return_once(move |_| Ok(returned_task));

    let cat = dill::CatalogBuilder::new()
        .add_value(task_svc_mock)
        .bind::<dyn TaskService, MockTaskService>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(format!(
            r#"{{
                tasks {{
                    getTask (taskId: "{}") {{
                        id
                        status
                        outcome
                    }}
                }}
            }}"#,
            expected_task.task_id,
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "getTask": {
                    "id": expected_task.task_id.to_string(),
                    "status": "FINISHED",
                    "outcome": "SUCCESS",
                },
            }
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_task_list_by_dataset() {
    let dataset_id = DatasetID::from_pub_key_ed25519(b"foo");

    let returned_task = TaskState {
        task_id: TaskID::new(123),
        status: TaskStatus::Queued,
        logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: dataset_id.clone(),
        }),
    };
    let expected_task = returned_task.clone();

    let mut task_svc_mock = MockTaskService::new();
    task_svc_mock
        .expect_list_tasks_by_dataset()
        .return_once(move |_| Box::pin(futures::stream::iter([Ok(returned_task)].into_iter())));

    let cat = dill::CatalogBuilder::new()
        .add_value(task_svc_mock)
        .bind::<dyn TaskService, MockTaskService>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(format!(
            r#"{{
                tasks {{
                    listTasksByDataset (datasetId: "{}") {{
                        nodes {{
                            id
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
            dataset_id,
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "tasks": {
                "listTasksByDataset": {
                    "nodes": [{
                        "id": expected_task.task_id.to_string(),
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
