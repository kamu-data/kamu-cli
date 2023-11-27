// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use futures::TryStreamExt;
use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;
use opendatafabric::*;

#[test_log::test(tokio::test)]
async fn test_event_store_empty() {
    let event_store = TaskSystemEventStoreInMemory::new();

    let events: Vec<_> = event_store
        .get_events(&TaskID::new(123), GetEventsOpts::default())
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
    let event_store = TaskSystemEventStoreInMemory::new();

    let task_id_1 = TaskID::new(123);
    let task_id_2 = TaskID::new(321);
    let dataset_id = DatasetID::from_pub_key_ed25519(b"foo");

    let event_1 = TaskCreated {
        event_time: Utc::now(),
        task_id: task_id_1,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2 = TaskCreated {
        event_time: Utc::now(),
        task_id: task_id_2,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_3 = TaskFinished {
        event_time: Utc::now(),
        task_id: task_id_1,
        outcome: TaskOutcome::Cancelled,
    };

    event_store
        .save_events(vec![
            event_1.clone().into(),
            event_2.clone().into(),
            event_3.clone().into(),
        ])
        .await
        .unwrap();

    let events: Vec<_> = event_store
        .get_events(&task_id_1, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1.into(), event_3.into()]);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(&dataset_id)
        .try_collect()
        .await
        .unwrap();

    // Ensure reverse chronological order
    assert_eq!(&tasks[..], [task_id_2, task_id_1]);
}
