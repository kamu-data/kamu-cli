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
use kamu_task_system_inmem::*;
use kamu_task_system_services::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_agg_create_new() {
    let event_store = InMemoryTaskEventStore::new();

    let metadata = TaskMetadata::from(vec![("foo", "x"), ("bar", "y")]);

    let mut task = Task::new(
        Utc::now(),
        event_store.new_task_id().await.unwrap(),
        LogicalPlanProbe::default().into(),
        Some(metadata.clone()),
    );

    assert_eq!(event_store.len().await.unwrap(), 0);

    task.save(&event_store).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 1);

    task.save(&event_store).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 1);

    let task = Task::load(task.task_id, &event_store).await.unwrap();
    assert_eq!(task.status(), TaskStatus::Queued);
    assert_eq!(
        task.logical_plan,
        LogicalPlan::Probe(LogicalPlanProbe::default())
    );
    assert_eq!(task.metadata, metadata);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_save_load_update() {
    let event_store = InMemoryTaskEventStore::new();
    let task_id = event_store.new_task_id().await.unwrap();

    let mut task = Task::new(
        Utc::now(),
        task_id,
        LogicalPlanProbe::default().into(),
        None,
    );
    task.save(&event_store).await.unwrap();

    task.run(Utc::now()).unwrap();
    task.cancel(Utc::now()).unwrap();

    task.save(&event_store).await.unwrap();
    let cancel_event = *task.last_stored_event_id().unwrap();
    assert_eq!(event_store.len().await.unwrap(), 3);

    task.finish(Utc::now(), TaskOutcome::Cancelled).unwrap();

    task.save(&event_store).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 4);

    // Full load
    let task = Task::load(task_id, &event_store).await.unwrap();
    assert_eq!(task.status(), TaskStatus::Finished);
    assert_eq!(task.outcome, Some(TaskOutcome::Cancelled));

    // Partial load
    let mut task = Task::load_ext(
        task_id,
        &event_store,
        LoadOpts {
            as_of_event: Some(cancel_event),
        },
    )
    .await
    .unwrap();
    assert_eq!(task.status(), TaskStatus::Running);
    assert!(task.cancellation_requested);

    // Update
    task.update(&event_store).await.unwrap();
    assert_eq!(task.status(), TaskStatus::Finished);
    assert_eq!(task.outcome, Some(TaskOutcome::Cancelled));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////#[test_log::test(tokio::test)]

#[test_log::test(tokio::test)]
async fn test_task_load_multi() {
    let event_store = InMemoryTaskEventStore::new();

    for _ in 0..5000 {
        let task_id = event_store.new_task_id().await.unwrap();
        let mut task = Task::new(
            Utc::now(),
            task_id,
            LogicalPlanProbe::default().into(),
            None,
        );
        task.save(&event_store).await.unwrap();
        task.run(Utc::now()).unwrap();
        task.cancel(Utc::now()).unwrap();
        task.save(&event_store).await.unwrap();
        task.finish(Utc::now(), TaskOutcome::Cancelled).unwrap();
        task.save(&event_store).await.unwrap();
    }

    let delta = 2500;
    let ids: Vec<TaskID> = (delta..(delta + 2000)).map(TaskID::new).collect();
    let tasks = Task::load_multi(ids, &event_store).await.unwrap();

    for (i, task_res) in tasks.into_iter().enumerate() {
        assert!(task_res.is_ok());
        let expected_id = TaskID::new(i as u64 + delta);
        let task = task_res.unwrap();
        assert_eq!(task.task_id, expected_id);
        assert_eq!(task.status(), TaskStatus::Finished);
        assert_eq!(task.outcome, Some(TaskOutcome::Cancelled));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_agg_illegal_transition() {
    let event_store = InMemoryTaskEventStore::new();

    let mut task = Task::new(
        Utc::now(),
        event_store.new_task_id().await.unwrap(),
        LogicalPlanProbe::default().into(),
        None,
    );
    task.finish(Utc::now(), TaskOutcome::Cancelled).unwrap();

    assert_matches!(task.run(Utc::now(),), Err(ProjectionError { .. }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_requeue() {
    let event_store = InMemoryTaskEventStore::new();

    let mut task = Task::new(
        Utc::now(),
        event_store.new_task_id().await.unwrap(),
        LogicalPlanProbe::default().into(),
        None,
    );
    task.run(Utc::now()).unwrap();
    task.save(&event_store).await.unwrap();
    assert_eq!(task.status(), TaskStatus::Running);

    task.requeue(Utc::now()).unwrap();
    task.save(&event_store).await.unwrap();
    assert_eq!(task.status(), TaskStatus::Queued);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
