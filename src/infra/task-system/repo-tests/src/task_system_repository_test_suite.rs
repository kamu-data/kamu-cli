// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_task_system::*;
use opendatafabric::DatasetID;

////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskSystemEventStore>().unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(0, num_events);

    let events: Vec<_> = event_store
        .get_events(&TaskID::new(123), GetEventsOpts::default())
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events, []);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(
            &DatasetID::new_seeded_ed25519(b"foo"),
            TaskPaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(tasks, []);
}

////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskSystemEventStore>().unwrap();

    let task_id_1 = TaskID::new(123);
    let task_id_2 = TaskID::new(321);
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_3 = TaskEventFinished {
        event_time: Utc::now(),
        task_id: task_id_1,
        outcome: TaskOutcome::Cancelled,
    };

    event_store
        .save_events(
            &task_id_1, // Cheating a bit,
            vec![
                event_1.clone().into(),
                event_2.clone().into(),
                event_3.clone().into(),
            ],
        )
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(3, num_events);

    let events: Vec<_> = event_store
        .get_events(&task_id_1, GetEventsOpts::default())
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1.into(), event_3.into()]);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id,
            TaskPaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    // Ensure reverse chronological order
    assert_eq!(&tasks[..], [task_id_2, task_id_1]);
}

////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskSystemEventStore>().unwrap();

    let task_id = event_store.new_task_id().await.unwrap();
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2 = TaskEventRunning {
        event_time: Utc::now(),
        task_id,
    };

    let event_3 = TaskEventFinished {
        event_time: Utc::now(),
        task_id,
        outcome: TaskOutcome::Cancelled,
    };

    let latest_event_id = event_store
        .save_events(
            &task_id,
            vec![
                event_1.clone().into(),
                event_2.clone().into(),
                event_3.clone().into(),
            ],
        )
        .await
        .unwrap();

    // Use "from" only
    let events: Vec<_> = event_store
        .get_events(
            &task_id,
            GetEventsOpts {
                from: Some(EventID::new(
                    latest_event_id.into_inner() - 2, /* last 2 events */
                )),
                to: None,
            },
        )
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.clone().into(), event_3.into()]);

    // Use "to" only
    let events: Vec<_> = event_store
        .get_events(
            &task_id,
            GetEventsOpts {
                from: None,
                to: Some(EventID::new(
                    latest_event_id.into_inner() - 1, /* first 2 events */
                )),
            },
        )
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1.into(), event_2.clone().into()]);

    // Use both "from" and "to"
    let events: Vec<_> = event_store
        .get_events(
            &task_id,
            GetEventsOpts {
                // From 1 to 2, middle event only
                from: Some(EventID::new(latest_event_id.into_inner() - 2)),
                to: Some(EventID::new(latest_event_id.into_inner() - 1)),
            },
        )
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.into()]);
}

////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_by_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskSystemEventStore>().unwrap();

    let task_id_1 = event_store.new_task_id().await.unwrap();
    let task_id_2 = event_store.new_task_id().await.unwrap();
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

    let event_1_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2,
        logical_plan: Probe {
            dataset_id: Some(dataset_id.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_1_2 = TaskEventRunning {
        event_time: Utc::now(),
        task_id: task_id_1,
    };

    let event_2_2 = TaskEventRunning {
        event_time: Utc::now(),
        task_id: task_id_2,
    };

    let event_1_3 = TaskEventFinished {
        event_time: Utc::now(),
        task_id: task_id_1,
        outcome: TaskOutcome::Cancelled,
    };

    let event_2_3 = TaskEventFinished {
        event_time: Utc::now(),
        task_id: task_id_2,
        outcome: TaskOutcome::Failed(TaskError::Empty),
    };

    event_store
        .save_events(
            &task_id_1,
            vec![
                event_1_1.clone().into(),
                event_1_2.clone().into(),
                event_1_3.clone().into(),
            ],
        )
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(3, num_events);

    event_store
        .save_events(
            &task_id_2,
            vec![
                event_2_1.clone().into(),
                event_2_2.clone().into(),
                event_2_3.clone().into(),
            ],
        )
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(6, num_events);

    let events: Vec<_> = event_store
        .get_events(&task_id_1, GetEventsOpts::default())
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        &events[..],
        [event_1_1.into(), event_1_2.into(), event_1_3.into()]
    );

    let events: Vec<_> = event_store
        .get_events(&task_id_2, GetEventsOpts::default())
        .await
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        &events[..],
        [event_2_1.into(), event_2_2.into(), event_2_3.into()]
    );
}

////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_dataset_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskSystemEventStore>().unwrap();

    let task_id_1_1 = event_store.new_task_id().await.unwrap();
    let task_id_2_1 = event_store.new_task_id().await.unwrap();
    let task_id_1_2 = event_store.new_task_id().await.unwrap();
    let task_id_2_2 = event_store.new_task_id().await.unwrap();

    let dataset_id_foo = DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_bar = DatasetID::new_seeded_ed25519(b"bar");

    let event_1_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1_1,
        logical_plan: Probe {
            dataset_id: Some(dataset_id_foo.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_1_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1_2,
        logical_plan: Probe {
            dataset_id: Some(dataset_id_foo.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2_1,
        logical_plan: Probe {
            dataset_id: Some(dataset_id_bar.clone()),
            ..Probe::default()
        }
        .into(),
    };

    let event_2_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2_2,
        logical_plan: Probe {
            dataset_id: Some(dataset_id_bar.clone()),
            ..Probe::default()
        }
        .into(),
    };

    event_store
        .save_events(&task_id_1_1, vec![event_1_1.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(1, num_events);

    event_store
        .save_events(&task_id_1_2, vec![event_1_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(2, num_events);

    event_store
        .save_events(&task_id_2_1, vec![event_2_1.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(3, num_events);

    event_store
        .save_events(&task_id_2_2, vec![event_2_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(4, num_events);

    let num_foo_tasks = event_store
        .get_count_tasks_by_dataset(&dataset_id_foo)
        .await
        .unwrap();
    assert_eq!(2, num_foo_tasks);

    let num_bar_tasks = event_store
        .get_count_tasks_by_dataset(&dataset_id_bar)
        .await
        .unwrap();
    assert_eq!(2, num_bar_tasks);

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            TaskPaginationOpts {
                limit: 5,
                offset: 0,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_2, task_id_1_1]); // Reverse order

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_bar,
            TaskPaginationOpts {
                limit: 5,
                offset: 0,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_2_2, task_id_2_1]); // Reverse order

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            TaskPaginationOpts {
                limit: 1,
                offset: 0,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_2]);

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            TaskPaginationOpts {
                limit: 1,
                offset: 1,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_1]);

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            TaskPaginationOpts {
                limit: 1,
                offset: 2,
            },
        )
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], []);
}

////////////////////////////////////////////////////////////////////////////////
