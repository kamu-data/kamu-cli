// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::Utc;
use database_common::PaginationOpts;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(0, num_events);

    let events: Vec<_> = event_store
        .get_events(&TaskID::new(123), GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events, []);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(tasks, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let task_id_1 = event_store.new_task_id().await.unwrap();
    let task_id_2 = event_store.new_task_id().await.unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_3 = TaskEventFinished {
        event_time: Utc::now(),
        task_id: task_id_1,
        outcome: TaskOutcome::Cancelled,
    };

    event_store
        .save_events(
            &task_id_1,
            None,
            vec![event_1.clone().into(), event_3.clone().into()],
        )
        .await
        .unwrap();

    event_store
        .save_events(&task_id_2, None, vec![event_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(3, num_events);

    let events: Vec<_> = event_store
        .get_events(&task_id_1, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1.into(), event_3.into()]);

    let tasks: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id,
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();

    // Ensure reverse chronological order
    assert_eq!(&tasks[..], [task_id_2, task_id_1]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let task_id = event_store.new_task_id().await.unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
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
            None,
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
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.into()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_by_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let task_id_1 = event_store.new_task_id().await.unwrap();
    let task_id_2 = event_store.new_task_id().await.unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let event_1_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_2_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
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
            None,
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
            None,
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
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        &events[..],
        [event_2_1.into(), event_2_2.into(), event_2_3.into()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_dataset_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let task_id_1_1 = event_store.new_task_id().await.unwrap();
    let task_id_2_1 = event_store.new_task_id().await.unwrap();
    let task_id_1_2 = event_store.new_task_id().await.unwrap();
    let task_id_2_2 = event_store.new_task_id().await.unwrap();

    let dataset_id_foo = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_bar = odf::DatasetID::new_seeded_ed25519(b"bar");

    let event_1_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1_1,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id_foo.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_1_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_1_2,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id_foo.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_2_1 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2_1,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id_bar.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    let event_2_2 = TaskEventCreated {
        event_time: Utc::now(),
        task_id: task_id_2_2,
        logical_plan: LogicalPlanProbe {
            dataset_id: Some(dataset_id_bar.clone()),
            ..LogicalPlanProbe::default()
        }
        .into(),
        metadata: None,
    };

    event_store
        .save_events(&task_id_1_1, None, vec![event_1_1.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(1, num_events);

    event_store
        .save_events(&task_id_1_2, None, vec![event_1_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(2, num_events);

    event_store
        .save_events(&task_id_2_1, None, vec![event_2_1.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
    assert_eq!(3, num_events);

    event_store
        .save_events(&task_id_2_2, None, vec![event_2_2.clone().into()])
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
            PaginationOpts {
                limit: 5,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_2, task_id_1_1]); // Reverse order

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_bar,
            PaginationOpts {
                limit: 5,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_2_2, task_id_2_1]); // Reverse order

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            PaginationOpts {
                limit: 1,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_2]);

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            PaginationOpts {
                limit: 1,
                offset: 1,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], [task_id_1_1]);

    let task_ids: Vec<_> = event_store
        .get_tasks_by_dataset(
            &dataset_id_foo,
            PaginationOpts {
                limit: 1,
                offset: 2,
            },
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&task_ids[..], []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_try_get_queued_single_task(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    // Initially, there is nothing to get
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert!(maybe_task_id.is_none());

    // Schedule a task
    let task_id_1 = event_store.new_task_id().await.unwrap();
    let last_event_id = event_store
        .save_events(
            &task_id_1,
            None,
            vec![TaskEventCreated {
                event_time: Utc::now(),
                task_id: task_id_1,
                logical_plan: LogicalPlanProbe::default().into(),
                metadata: None,
            }
            .into()],
        )
        .await
        .unwrap();

    // The only queued task should be returned
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_id_1));

    // Mark the task as running
    let last_event_id = event_store
        .save_events(
            &task_id_1,
            Some(last_event_id),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id: task_id_1,
            }
            .into()],
        )
        .await
        .unwrap();

    // Right now nothing should be visible
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert!(maybe_task_id.is_none());

    // Requeue the task (server restarted)
    let last_event_id = event_store
        .save_events(
            &task_id_1,
            Some(last_event_id),
            vec![TaskEventRequeued {
                event_time: Utc::now(),
                task_id: task_id_1,
            }
            .into()],
        )
        .await
        .unwrap();

    // The task should be visible again
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_id_1));

    // Now run and finish the task
    event_store
        .save_events(
            &task_id_1,
            Some(last_event_id),
            vec![
                TaskEventRunning {
                    event_time: Utc::now(),
                    task_id: task_id_1,
                }
                .into(),
                TaskEventFinished {
                    event_time: Utc::now(),
                    task_id: task_id_1,
                    outcome: TaskOutcome::Success(TaskResult::Empty),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // The task should disappear again
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert!(maybe_task_id.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_try_get_queued_multiple_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    // Schedule a few tasks
    let mut task_ids = Vec::new();
    let mut last_event_ids = Vec::new();
    for _ in 0..3 {
        let task_id = event_store.new_task_id().await.unwrap();
        let last_event_id = event_store
            .save_events(
                &task_id,
                None,
                vec![TaskEventCreated {
                    event_time: Utc::now(),
                    task_id,
                    logical_plan: LogicalPlanProbe::default().into(),
                    metadata: None,
                }
                .into()],
            )
            .await
            .unwrap();

        task_ids.push(task_id);
        last_event_ids.push(last_event_id);
    }

    // We should see the earliest registered task
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_ids[0]));

    // Mark task 0 as running
    last_event_ids[0] = event_store
        .save_events(
            &task_ids[0],
            Some(last_event_ids[0]),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id: task_ids[0],
            }
            .into()],
        )
        .await
        .unwrap();

    // Now we should see the next registered task
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_ids[1]));

    // Mark task 1 as running, then finished
    last_event_ids[1] = event_store
        .save_events(
            &task_ids[1],
            Some(last_event_ids[1]),
            vec![
                TaskEventRunning {
                    event_time: Utc::now(),
                    task_id: task_ids[1],
                }
                .into(),
                TaskEventFinished {
                    event_time: Utc::now(),
                    task_id: task_ids[1],
                    outcome: TaskOutcome::Success(TaskResult::Empty),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Now we should see the last registered task
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_ids[2]));

    // Task 0 got requeued
    last_event_ids[0] = event_store
        .save_events(
            &task_ids[0],
            Some(last_event_ids[0]),
            vec![TaskEventRequeued {
                event_time: Utc::now(),
                task_id: task_ids[0],
            }
            .into()],
        )
        .await
        .unwrap();

    // This should bring task 0 back to the top of the queue
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_ids[0]));

    // Mark task 0 as running, then finished
    last_event_ids[0] = event_store
        .save_events(
            &task_ids[0],
            Some(last_event_ids[0]),
            vec![
                TaskEventRunning {
                    event_time: Utc::now(),
                    task_id: task_ids[0],
                }
                .into(),
                TaskEventFinished {
                    event_time: Utc::now(),
                    task_id: task_ids[0],
                    outcome: TaskOutcome::Success(TaskResult::Empty),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Task 2 should be the top again
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert_eq!(maybe_task_id, Some(task_ids[2]));

    // Mark task 2 as running
    last_event_ids[2] = event_store
        .save_events(
            &task_ids[2],
            Some(last_event_ids[2]),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id: task_ids[2],
            }
            .into()],
        )
        .await
        .unwrap();

    // We should see empty queue
    let maybe_task_id = event_store.try_get_queued_task().await.unwrap();
    assert!(maybe_task_id.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_running_tasks(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    // No running tasks initially

    let running_count = event_store.get_count_running_tasks().await.unwrap();
    assert_eq!(running_count, 0);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert!(running_task_ids.is_empty());

    // Schedule a few tasks
    let mut task_ids = Vec::new();
    let mut last_event_ids = Vec::new();
    for _ in 0..3 {
        let task_id = event_store.new_task_id().await.unwrap();
        let last_event_id = event_store
            .save_events(
                &task_id,
                None,
                vec![TaskEventCreated {
                    event_time: Utc::now(),
                    task_id,
                    logical_plan: LogicalPlanProbe::default().into(),
                    metadata: None,
                }
                .into()],
            )
            .await
            .unwrap();

        task_ids.push(task_id);
        last_event_ids.push(last_event_id);
    }

    // Still no running tasks

    let running_count = event_store.get_count_running_tasks().await.unwrap();
    assert_eq!(running_count, 0);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert!(running_task_ids.is_empty());

    // Mark 2 of 3 tasks as running
    last_event_ids[0] = event_store
        .save_events(
            &task_ids[0],
            Some(last_event_ids[0]),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id: task_ids[0],
            }
            .into()],
        )
        .await
        .unwrap();
    last_event_ids[1] = event_store
        .save_events(
            &task_ids[1],
            Some(last_event_ids[1]),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id: task_ids[1],
            }
            .into()],
        )
        .await
        .unwrap();

    // Should see 2 running tasks

    let running_count = event_store.get_count_running_tasks().await.unwrap();
    assert_eq!(running_count, 2);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(running_task_ids, vec![task_ids[0], task_ids[1]]);

    // Query the same state with pagination args

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 1,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(running_task_ids, vec![task_ids[0]]);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 2,
            offset: 1,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(running_task_ids, vec![task_ids[1]]);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 2,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(running_task_ids, vec![]);

    // Finish 2nd task only
    last_event_ids[1] = event_store
        .save_events(
            &task_ids[1],
            Some(last_event_ids[1]),
            vec![TaskEventFinished {
                event_time: Utc::now(),
                task_id: task_ids[1],
                outcome: TaskOutcome::Success(TaskResult::Empty),
            }
            .into()],
        )
        .await
        .unwrap();

    // Should see only the first running task

    let running_count = event_store.get_count_running_tasks().await.unwrap();
    assert_eq!(running_count, 1);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(running_task_ids, vec![task_ids[0]]);

    // Requeue 1st task
    last_event_ids[0] = event_store
        .save_events(
            &task_ids[0],
            Some(last_event_ids[0]),
            vec![TaskEventRequeued {
                event_time: Utc::now(),
                task_id: task_ids[0],
            }
            .into()],
        )
        .await
        .unwrap();

    // No running task after this, just 2 queued (#0, #2) and 1 finished (#1)

    let running_count = event_store.get_count_running_tasks().await.unwrap();
    assert_eq!(running_count, 0);

    let running_task_ids: Vec<_> = event_store
        .get_running_tasks(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .try_collect()
        .await
        .unwrap();
    assert!(running_task_ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_concurrent_modification(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn TaskEventStore>().unwrap();

    let task_id = event_store.new_task_id().await.unwrap();

    // Nothing stored yet, but prev stored event id sent => CM
    let res = event_store
        .save_events(
            &task_id,
            Some(EventID::new(15)),
            vec![TaskEventCreated {
                event_time: Utc::now(),
                task_id,
                logical_plan: LogicalPlanProbe::default().into(),
                metadata: None,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Nothing stored yet, no storage expectation => OK
    let res = event_store
        .save_events(
            &task_id,
            None,
            vec![TaskEventCreated {
                event_time: Utc::now(),
                task_id,
                logical_plan: LogicalPlanProbe::default().into(),
                metadata: None,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Ok(_));

    // Something stored, but no expectation => CM
    let res = event_store
        .save_events(
            &task_id,
            None,
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, but expectation is wrong => CM
    let res = event_store
        .save_events(
            &task_id,
            Some(EventID::new(15)),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, and expectation is correct
    let res = event_store
        .save_events(
            &task_id,
            Some(EventID::new(1)),
            vec![TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
