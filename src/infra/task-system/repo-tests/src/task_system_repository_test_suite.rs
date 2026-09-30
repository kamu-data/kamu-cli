// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    assert_eq!(harness.total_events().await, 0);
    assert_eq!(harness.task_events(TaskID::new(123)).await, []);
    assert_eq!(
        harness
            .dataset_task_ids(
                &odf::DatasetID::new_seeded_ed25519(b"foo"),
                PaginationOpts::from_max_results(100)
            )
            .await,
        []
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_id_1 = harness.new_task_id().await;
    let task_id_2 = harness.new_task_id().await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = harness.created(task_id_1, Some(&dataset_id));
    let event_2 = harness.created(task_id_2, Some(&dataset_id));
    let event_3 = harness.finished(task_id_1, TaskOutcome::Cancelled);

    harness
        .save(task_id_1, vec![event_1.clone(), event_3.clone()])
        .await;
    harness.save(task_id_2, vec![event_2]).await;

    assert_eq!(harness.total_events().await, 3);
    assert_eq!(harness.task_events(task_id_1).await, [event_1, event_3]);

    // Ensure reverse chronological order
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id, PaginationOpts::from_max_results(100))
            .await,
        [task_id_2, task_id_1]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_id = harness.new_task_id().await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let event_1 = harness.created(task_id, Some(&dataset_id));
    let event_2 = harness.running(task_id);
    let event_3 = harness.finished(task_id, TaskOutcome::Cancelled);

    let latest_event_id = harness
        .save(
            task_id,
            vec![event_1.clone(), event_2.clone(), event_3.clone()],
        )
        .await
        .into_inner();

    // Use "from" only: last 2 events
    let events = harness
        .task_events_windowed(
            task_id,
            GetEventsOpts {
                from: Some(EventID::new(latest_event_id - 2)),
                to: None,
            },
        )
        .await;
    assert_eq!(events, [event_2.clone(), event_3]);

    // Use "to" only: first 2 events
    let events = harness
        .task_events_windowed(
            task_id,
            GetEventsOpts {
                from: None,
                to: Some(EventID::new(latest_event_id - 1)),
            },
        )
        .await;
    assert_eq!(events, [event_1, event_2.clone()]);

    // Use both "from" and "to": middle event only
    let events = harness
        .task_events_windowed(
            task_id,
            GetEventsOpts {
                from: Some(EventID::new(latest_event_id - 2)),
                to: Some(EventID::new(latest_event_id - 1)),
            },
        )
        .await;
    assert_eq!(events, [event_2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_by_tasks(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_id_1 = harness.new_task_id().await;
    let task_id_2 = harness.new_task_id().await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let events_1 = vec![
        harness.created(task_id_1, Some(&dataset_id)),
        harness.running(task_id_1),
        harness.finished(task_id_1, TaskOutcome::Cancelled),
    ];
    let events_2 = vec![
        harness.created(task_id_2, Some(&dataset_id)),
        harness.running(task_id_2),
        harness.finished(
            task_id_2,
            TaskOutcome::Failed(TaskError::empty_recoverable()),
        ),
    ];

    harness.save(task_id_1, events_1.clone()).await;
    assert_eq!(harness.total_events().await, 3);

    harness.save(task_id_2, events_2.clone()).await;
    assert_eq!(harness.total_events().await, 6);

    assert_eq!(harness.task_events(task_id_1).await, events_1);
    assert_eq!(harness.task_events(task_id_2).await, events_2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_dataset_tasks(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_id_1_1 = harness.new_task_id().await;
    let task_id_2_1 = harness.new_task_id().await;
    let task_id_1_2 = harness.new_task_id().await;
    let task_id_2_2 = harness.new_task_id().await;

    let dataset_id_foo = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_bar = odf::DatasetID::new_seeded_ed25519(b"bar");

    for (i, (task_id, dataset_id)) in [
        (task_id_1_1, &dataset_id_foo),
        (task_id_1_2, &dataset_id_foo),
        (task_id_2_1, &dataset_id_bar),
        (task_id_2_2, &dataset_id_bar),
    ]
    .into_iter()
    .enumerate()
    {
        harness
            .save(task_id, vec![harness.created(task_id, Some(dataset_id))])
            .await;
        assert_eq!(harness.total_events().await, i + 1);
    }

    assert_eq!(harness.dataset_task_count(&dataset_id_foo).await, 2);
    assert_eq!(harness.dataset_task_count(&dataset_id_bar).await, 2);

    // Reverse order
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id_foo, PaginationOpts::from_max_results(5))
            .await,
        [task_id_1_2, task_id_1_1]
    );
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id_bar, PaginationOpts::from_max_results(5))
            .await,
        [task_id_2_2, task_id_2_1]
    );

    // Pagination
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id_foo, PaginationOpts::from_max_results(1))
            .await,
        [task_id_1_2]
    );
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id_foo, PaginationOpts::from_page(1, 1))
            .await,
        [task_id_1_1]
    );
    assert_eq!(
        harness
            .dataset_task_ids(&dataset_id_foo, PaginationOpts::from_page(2, 1))
            .await,
        []
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_try_get_queued_single_task(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    // Initially, there is nothing to get
    assert_eq!(harness.queued_task().await, None);

    // The only queued task should be returned
    let task_id = harness.create_task().await;
    assert_eq!(harness.queued_task().await, Some(task_id));

    // Running: nothing is queued
    harness.save(task_id, vec![harness.running(task_id)]).await;
    assert_eq!(harness.queued_task().await, None);

    // Requeued (server restarted): visible again
    harness.save(task_id, vec![harness.requeued(task_id)]).await;
    assert_eq!(harness.queued_task().await, Some(task_id));

    // Run and finished: gone again
    harness
        .save(
            task_id,
            vec![
                harness.running(task_id),
                harness.finished(task_id, TaskOutcome::Success(TaskResult::empty())),
            ],
        )
        .await;
    assert_eq!(harness.queued_task().await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_try_get_queued_multiple_tasks(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_ids = [
        harness.create_task().await,
        harness.create_task().await,
        harness.create_task().await,
    ];

    // We should see the earliest registered task
    assert_eq!(harness.queued_task().await, Some(task_ids[0]));

    // Task 0 running: the next registered task is seen
    harness
        .save(task_ids[0], vec![harness.running(task_ids[0])])
        .await;
    assert_eq!(harness.queued_task().await, Some(task_ids[1]));

    // Task 1 running, then finished: the last registered task is seen
    harness
        .save(
            task_ids[1],
            vec![
                harness.running(task_ids[1]),
                harness.finished(task_ids[1], TaskOutcome::Success(TaskResult::empty())),
            ],
        )
        .await;
    assert_eq!(harness.queued_task().await, Some(task_ids[2]));

    // Task 0 requeued earlier than task 2 was queued: back to the top of the queue
    harness
        .save(
            task_ids[0],
            vec![harness.requeued_at(task_ids[0], Utc::now() - Duration::seconds(1))],
        )
        .await;
    assert_eq!(harness.queued_task().await, Some(task_ids[0]));

    // Task 0 running, then finished: task 2 is the top again
    harness
        .save(
            task_ids[0],
            vec![
                harness.running(task_ids[0]),
                harness.finished(task_ids[0], TaskOutcome::Success(TaskResult::empty())),
            ],
        )
        .await;
    assert_eq!(harness.queued_task().await, Some(task_ids[2]));

    // Task 2 running: the queue is empty
    harness
        .save(task_ids[2], vec![harness.running(task_ids[2])])
        .await;
    assert_eq!(harness.queued_task().await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_running_tasks(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    // No running tasks initially
    assert_eq!(harness.running_task_count().await, 0);
    assert_eq!(harness.running_task_ids().await, []);

    let task_ids = [
        harness.create_task().await,
        harness.create_task().await,
        harness.create_task().await,
    ];

    // Still no running tasks
    assert_eq!(harness.running_task_count().await, 0);
    assert_eq!(harness.running_task_ids().await, []);

    // Mark 2 of 3 tasks as running
    harness
        .save(task_ids[0], vec![harness.running(task_ids[0])])
        .await;
    harness
        .save(task_ids[1], vec![harness.running(task_ids[1])])
        .await;

    assert_eq!(harness.running_task_count().await, 2);
    assert_eq!(harness.running_task_ids().await, [task_ids[0], task_ids[1]]);

    // Query the same state with pagination args
    assert_eq!(
        harness
            .running_task_ids_paged(PaginationOpts::from_max_results(1))
            .await,
        [task_ids[0]]
    );
    assert_eq!(
        harness
            .running_task_ids_paged(PaginationOpts {
                limit: 2,
                offset: 1,
            })
            .await,
        [task_ids[1]]
    );
    assert_eq!(
        harness
            .running_task_ids_paged(PaginationOpts {
                limit: 100,
                offset: 2,
            })
            .await,
        []
    );

    // Finish 2nd task only: only the first one is running
    harness
        .save(
            task_ids[1],
            vec![harness.finished(task_ids[1], TaskOutcome::Success(TaskResult::empty()))],
        )
        .await;
    assert_eq!(harness.running_task_count().await, 1);
    assert_eq!(harness.running_task_ids().await, [task_ids[0]]);

    // Requeue 1st task: none running, 2 queued (#0, #2) and 1 finished (#1)
    harness
        .save(task_ids[0], vec![harness.requeued(task_ids[0])])
        .await;
    assert_eq!(harness.running_task_count().await, 0);
    assert_eq!(harness.running_task_ids().await, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_task_status_on_cancellation(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    // Cancelling a queued task finishes it
    let queued_task_id = harness.create_task().await;
    assert_eq!(harness.queued_task().await, Some(queued_task_id));
    harness
        .save(queued_task_id, vec![harness.cancelled(queued_task_id)])
        .await;
    assert_eq!(harness.queued_task().await, None);
    assert_eq!(harness.running_task_ids().await, vec![]);

    // Cancelling a running task leaves it running: the run cannot be interrupted
    let running_task_id = harness.create_task().await;
    harness
        .save(running_task_id, vec![harness.running(running_task_id)])
        .await;
    harness
        .save(running_task_id, vec![harness.cancelled(running_task_id)])
        .await;
    assert_eq!(harness.queued_task().await, None);
    assert_eq!(harness.running_task_ids().await, vec![running_task_id]);

    // Until it finishes
    harness
        .save(
            running_task_id,
            vec![harness.finished(running_task_id, TaskOutcome::Cancelled)],
        )
        .await;
    assert_eq!(harness.running_task_ids().await, vec![]);

    // Cancelled in the same save as it started running: still running
    let batch_task_id = harness.create_task().await;
    harness
        .save(
            batch_task_id,
            vec![
                harness.running(batch_task_id),
                harness.cancelled(batch_task_id),
            ],
        )
        .await;
    assert_eq!(harness.queued_task().await, None);
    assert_eq!(harness.running_task_ids().await, vec![batch_task_id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_concurrent_modification(catalog: &Catalog) {
    let harness = TaskEventStoreTestSuiteHarness::new(catalog);

    let task_id = harness.new_task_id().await;

    // Nothing stored yet, but prev stored event id sent => CM
    let res = harness
        .try_save(
            task_id,
            Some(EventID::new(15)),
            vec![harness.created(task_id, None)],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Nothing stored yet, no storage expectation => OK
    let res = harness
        .try_save(task_id, None, vec![harness.created(task_id, None)])
        .await;
    assert_matches!(res, Ok(_));

    // Something stored, but no expectation => CM
    let res = harness
        .try_save(task_id, None, vec![harness.running(task_id)])
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, but expectation is wrong => CM
    let res = harness
        .try_save(
            task_id,
            Some(EventID::new(15)),
            vec![harness.running(task_id)],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, and expectation is correct
    let res = harness
        .try_save(
            task_id,
            Some(EventID::new(1)),
            vec![harness.running(task_id)],
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TaskEventStoreTestSuiteHarness {
    event_store: Arc<dyn TaskEventStore>,
    last_event_ids: Mutex<HashMap<TaskID, EventID>>,
}

impl TaskEventStoreTestSuiteHarness {
    fn new(catalog: &Catalog) -> Self {
        Self {
            event_store: catalog.get_one().unwrap(),
            last_event_ids: Mutex::new(HashMap::new()),
        }
    }

    async fn new_task_id(&self) -> TaskID {
        self.event_store.new_task_id().await.unwrap()
    }

    /// Creates a queued task without a dataset
    async fn create_task(&self) -> TaskID {
        let task_id = self.new_task_id().await;
        self.save(task_id, vec![self.created(task_id, None)]).await;
        task_id
    }

    /// Saves events after the ones this harness saved before for the task
    async fn save(&self, task_id: TaskID, events: Vec<TaskEvent>) -> EventID {
        let maybe_prev_stored_event_id = self.last_event_ids.lock().unwrap().get(&task_id).copied();
        let last_event_id = self
            .try_save(task_id, maybe_prev_stored_event_id, events)
            .await
            .unwrap();
        self.last_event_ids
            .lock()
            .unwrap()
            .insert(task_id, last_event_id);
        last_event_id
    }

    async fn try_save(
        &self,
        task_id: TaskID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        self.event_store
            .save_events(&task_id, maybe_prev_stored_event_id, events)
            .await
    }

    async fn total_events(&self) -> usize {
        self.event_store.total_events_stored().await.unwrap()
    }

    async fn task_events(&self, task_id: TaskID) -> Vec<TaskEvent> {
        self.task_events_windowed(task_id, GetEventsOpts::default())
            .await
    }

    async fn task_events_windowed(&self, task_id: TaskID, opts: GetEventsOpts) -> Vec<TaskEvent> {
        self.event_store
            .get_events(&task_id, opts)
            .map_ok(|(_, event)| event)
            .try_collect()
            .await
            .unwrap()
    }

    async fn dataset_task_ids(
        &self,
        dataset_id: &odf::DatasetID,
        pagination: PaginationOpts,
    ) -> Vec<TaskID> {
        self.event_store
            .get_tasks_by_dataset(dataset_id, pagination)
            .try_collect()
            .await
            .unwrap()
    }

    async fn dataset_task_count(&self, dataset_id: &odf::DatasetID) -> usize {
        self.event_store
            .get_count_tasks_by_dataset(dataset_id)
            .await
            .unwrap()
    }

    async fn queued_task(&self) -> Option<TaskID> {
        self.event_store.try_get_queued_task().await.unwrap()
    }

    async fn running_task_count(&self) -> usize {
        self.event_store.get_count_running_tasks().await.unwrap()
    }

    async fn running_task_ids(&self) -> Vec<TaskID> {
        self.running_task_ids_paged(PaginationOpts::from_max_results(100))
            .await
    }

    async fn running_task_ids_paged(&self, pagination: PaginationOpts) -> Vec<TaskID> {
        self.event_store
            .get_running_tasks(pagination)
            .try_collect()
            .await
            .unwrap()
    }

    fn created(&self, task_id: TaskID, maybe_dataset_id: Option<&odf::DatasetID>) -> TaskEvent {
        TaskEventCreated {
            event_time: Utc::now(),
            task_id,
            logical_plan: LogicalPlanProbe {
                dataset_id: maybe_dataset_id.cloned(),
                ..LogicalPlanProbe::default()
            }
            .into_logical_plan(),
            metadata: None,
        }
        .into()
    }

    fn running(&self, task_id: TaskID) -> TaskEvent {
        TaskEventRunning {
            event_time: Utc::now(),
            task_id,
        }
        .into()
    }

    fn requeued(&self, task_id: TaskID) -> TaskEvent {
        self.requeued_at(task_id, Utc::now())
    }

    fn requeued_at(&self, task_id: TaskID, event_time: DateTime<Utc>) -> TaskEvent {
        TaskEventRequeued {
            event_time,
            task_id,
        }
        .into()
    }

    fn cancelled(&self, task_id: TaskID) -> TaskEvent {
        TaskEventCancelled {
            event_time: Utc::now(),
            task_id,
        }
        .into()
    }

    fn finished(&self, task_id: TaskID, outcome: TaskOutcome) -> TaskEvent {
        TaskEventFinished {
            event_time: Utc::now(),
            task_id,
            outcome,
        }
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
