// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_domain_task_system::*;
use kamu_infra_task_system_in_memory::*;
use opendatafabric::DatasetID;

#[test_log::test(tokio::test)]
async fn test_task_get_non_existing() {
    let cat = dill::CatalogBuilder::new()
        .add::<TaskServiceInMemory>()
        .bind::<dyn TaskService, TaskServiceInMemory>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(
            r#"{
                tasks {
                    getTask (taskId: "123") {
                        taskId
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
    let cat = dill::CatalogBuilder::new()
        .add::<TaskServiceInMemory>()
        .bind::<dyn TaskService, TaskServiceInMemory>()
        .build();

    let task_svc = cat.get_one::<dyn TaskService>().unwrap();
    let expected_task = task_svc
        .create_task(LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: DatasetID::from_pub_key_ed25519(b"foo"),
        }))
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(format!(
            r#"{{
                tasks {{
                    getTask (taskId: "{}") {{
                        taskId
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
                    "taskId": expected_task.task_id.to_string(),
                },
            }
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_task_list_by_dataset() {
    let cat = dill::CatalogBuilder::new()
        .add::<TaskServiceInMemory>()
        .bind::<dyn TaskService, TaskServiceInMemory>()
        .build();

    let dataset_id = DatasetID::from_pub_key_ed25519(b"foo");
    let task_svc = cat.get_one::<dyn TaskService>().unwrap();
    let expected_task = task_svc
        .create_task(LogicalPlan::UpdateDataset(UpdateDataset {
            dataset_id: dataset_id.clone(),
        }))
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(format!(
            r#"{{
                tasks {{
                    listTasksByDataset (datasetId: "{}") {{
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
