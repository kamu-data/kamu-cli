// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use futures::TryStreamExt;
use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;
use opendatafabric::*;

#[test_log::test(tokio::test)]
async fn test_event_store_empty() {
    let event_store = TaskEventStoreInMemory::new();

    let events: Vec<_> = event_store
        .get_events_by_task(&TaskID::new(123), None, None)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events, []);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(&DatasetID::from_pub_key_ed25519(b"foo"))
        .try_collect()
        .await
        .unwrap();

    assert_eq!(tasks, []);
}

#[test_log::test(tokio::test)]
async fn test_event_store_get_streams() {
    let event_store = TaskEventStoreInMemory::new();

    let task_id = TaskID::new(123);
    let dataset_id = DatasetID::from_pub_key_ed25519(b"foo");
    let event_expected = TaskCreated {
        task_id,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    event_store
        .save_event(event_expected.clone().into())
        .await
        .unwrap();

    let events: Vec<_> = event_store
        .get_events_by_task(&task_id, None, None)
        .try_collect()
        .await
        .unwrap();

    assert_matches!(
        &events[..],
        [
            TaskEvent::Created(TaskCreated {
                task_id,
                logical_plan,
            })
        ] if *task_id == event_expected.task_id && *logical_plan == event_expected.logical_plan
    );

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(&dataset_id)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&tasks[..], [task_id]);
}
