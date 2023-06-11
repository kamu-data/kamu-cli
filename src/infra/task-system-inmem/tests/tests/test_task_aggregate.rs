// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;

#[test_log::test(tokio::test)]
async fn test_task_agg_create_new() {
    let event_store = TaskEventStoreInMemory::new();

    let mut task = Task::new(event_store.new_task_id(), Probe::default().into());

    assert_eq!(event_store.len().await.unwrap(), 0);

    event_store.save(&mut task).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 1);

    event_store.save(&mut task).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 1);

    let task = event_store.load(&task.task_id).await.unwrap();
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(task.logical_plan, LogicalPlan::Probe(Probe::default()));
}

#[test_log::test(tokio::test)]
async fn test_task_save_load_update() {
    let event_store = TaskEventStoreInMemory::new();
    let task_id = event_store.new_task_id();

    let mut task = Task::new(task_id, Probe::default().into());
    event_store.save(&mut task).await.unwrap();

    task.run().unwrap();
    task.cancel().unwrap();

    event_store.save(&mut task).await.unwrap();
    let cancel_event = *task.last_synced_event().unwrap();
    assert_eq!(event_store.len().await.unwrap(), 3);

    task.finish(TaskOutcome::Cancelled).unwrap();

    event_store.save(&mut task).await.unwrap();
    assert_eq!(event_store.len().await.unwrap(), 4);

    // Full load
    let task = event_store.load(&task_id).await.unwrap();
    assert_eq!(task.status, TaskStatus::Finished(TaskOutcome::Cancelled));

    // Partial load
    let mut task = event_store
        .load_ext(
            &task_id,
            LoadOpts {
                as_of_event: Some(cancel_event),
            },
        )
        .await
        .unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.cancellation_requested, true);

    // Update
    event_store.update(&mut task).await.unwrap();
    assert_eq!(task.status, TaskStatus::Finished(TaskOutcome::Cancelled));
}

#[test_log::test(tokio::test)]
async fn test_task_agg_illegal_transition() {
    let event_store = TaskEventStoreInMemory::new();

    let mut task = Task::new(event_store.new_task_id(), Probe::default().into());
    task.finish(TaskOutcome::Cancelled).unwrap();

    assert_matches!(task.run(), Err(IllegalSequenceError { .. }));
}
