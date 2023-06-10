// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use futures::StreamExt;
use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;

#[test_log::test(tokio::test)]
async fn test_task_agg_create_new() {
    let event_store = TaskEventStoreInMemory::new();

    let mut task = Task::new(&event_store, Probe::default().into());

    assert_eq!(
        event_store
            .get_events_by_task(task.id(), None, None)
            .count()
            .await,
        0
    );

    task.save(&event_store).await.unwrap();
    assert_eq!(
        event_store
            .get_events_by_task(task.id(), None, None)
            .count()
            .await,
        1
    );

    task.save(&event_store).await.unwrap();
    assert_eq!(
        event_store
            .get_events_by_task(task.id(), None, None)
            .count()
            .await,
        1
    );

    let task = Task::load(task.id(), &event_store).await.unwrap().unwrap();
    assert_eq!(*task.status(), TaskStatus::Queued);
    assert_eq!(*task.logical_plan(), LogicalPlan::Probe(Probe::default()));
}

#[test_log::test(tokio::test)]
async fn test_task_agg_illegal_transition() {
    let event_store = TaskEventStoreInMemory::new();

    let mut task = Task::new(&event_store, Probe::default().into());
    task.finish(TaskOutcome::Cancelled).unwrap();

    assert_matches!(task.set_running(), Err(IllegalSequenceError { .. }));
}
