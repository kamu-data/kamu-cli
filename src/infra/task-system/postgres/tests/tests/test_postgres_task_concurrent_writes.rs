// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::time::Duration;

use chrono::Utc;
use database_common::TransactionRefT;
use dill::{Catalog, CatalogBuilder};
use kamu_task_system::*;
use kamu_task_system_postgres::PostgresTaskEventStore;
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_bulk_task_update_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentTransactionsHarness::new(pg_pool);

    let (task_id, created_event_id) = harness.create_committed_task().await;

    let res = harness
        .run_task_in_bulk_in_racing_transactions(task_id, created_event_id)
        .await;

    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConcurrentTransactionsHarness {
    pg_pool: PgPool,
}

impl ConcurrentTransactionsHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }

    async fn create_committed_task(&self) -> (TaskID, EventID) {
        let transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let event_store = Self::catalog_for(&transaction)
            .get_one::<dyn TaskEventStore>()
            .unwrap();

        let task_id = event_store.new_task_id().await.unwrap();
        let created_event_id = event_store
            .save_events(
                &task_id,
                None,
                vec![
                    TaskEventCreated {
                        event_time: Utc::now(),
                        task_id,
                        logical_plan: LogicalPlanProbe::default().into_logical_plan(),
                        metadata: None,
                    }
                    .into(),
                ],
            )
            .await
            .unwrap();
        drop(event_store);

        transaction
            .into_inner_db_transaction()
            .unwrap()
            .commit()
            .await
            .unwrap();

        (task_id, created_event_id)
    }

    /// Both transactions mark the task as running, the second one next to a
    /// new task of its own
    async fn run_task_in_bulk_in_racing_transactions(
        &self,
        task_id: TaskID,
        created_event_id: EventID,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        let first_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let second_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        let running_item = SaveEventsItem {
            query: task_id,
            maybe_prev_stored_event_id: Some(created_event_id),
            events: vec![
                TaskEventRunning {
                    event_time: Utc::now(),
                    task_id,
                }
                .into(),
            ],
        };

        let first_event_store = Self::catalog_for(&first_transaction)
            .get_one::<dyn TaskEventStore>()
            .unwrap();
        first_event_store
            .save_events_multi(vec![running_item.clone()])
            .await
            .unwrap();
        drop(first_event_store);

        let second_event_store = Self::catalog_for(&second_transaction)
            .get_one::<dyn TaskEventStore>()
            .unwrap();
        let new_task_id = second_event_store.new_task_id().await.unwrap();
        let new_task_item = SaveEventsItem {
            query: new_task_id,
            maybe_prev_stored_event_id: None,
            events: vec![
                TaskEventCreated {
                    event_time: Utc::now(),
                    task_id: new_task_id,
                    logical_plan: LogicalPlanProbe::default().into_logical_plan(),
                    metadata: None,
                }
                .into(),
            ],
        };
        let second_save = tokio::spawn(async move {
            second_event_store
                .save_events_multi(vec![new_task_item, running_item])
                .await
        });

        // Let the second save reach the database before the first commits
        tokio::time::sleep(Duration::from_millis(200)).await;

        first_transaction
            .into_inner_db_transaction()
            .unwrap()
            .commit()
            .await
            .unwrap();

        second_save.await.unwrap()
    }

    fn catalog_for(transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new();
        transaction.clone().into_erased().register(&mut b);
        b.add::<PostgresTaskEventStore>();
        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
