// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::TransactionRefT;
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system::*;
use kamu_flow_system_postgres::{PostgresFlowSystemEventBridge, PostgresFlowTriggerEventStore};
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::{PgPool, Postgres};
use tokio::time::{Duration, Instant};
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PROJECTOR_NAME: &str = "test-projector";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_later_transaction_waits_for_earlier_in_flight_one(pg_pool: PgPool) {
    let harness = FlowSystemEventBridgeHarness::new(pg_pool);

    // The slow transaction writes first, so it holds the lower transaction ID
    let slow_transaction = harness.begin();
    let slow_event_id = harness.create_trigger(&slow_transaction, "slow").await;

    // A later transaction commits while the slow one is in flight
    let fast_transaction = harness.begin();
    let fast_event_id = harness.create_trigger(&fast_transaction, "fast").await;
    FlowSystemEventBridgeHarness::commit(fast_transaction).await;

    // Delivering it now would move the cursor past the slow transaction
    assert_eq!(harness.project_next_batch().await, []);
    assert!(harness.has_held_back_events().await);

    FlowSystemEventBridgeHarness::commit(slow_transaction).await;

    // Transactions of other tests on the cluster may delay, never reorder
    assert_eq!(
        harness.project_until_count(2).await,
        [slow_event_id, fast_event_id]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowSystemEventBridgeHarness {
    pg_pool: PgPool,
    base_catalog: Catalog,
}

impl FlowSystemEventBridgeHarness {
    fn new(pg_pool: PgPool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(pg_pool.clone());
        b.add::<PostgresNotificationHub>();
        b.add::<WakeupListenerMetrics>();
        b.add::<PostgresFlowSystemEventBridge>();
        b.add::<PostgresFlowTriggerEventStore>();

        Self {
            pg_pool,
            base_catalog: b.build(),
        }
    }

    fn begin(&self) -> TransactionRefT<Postgres> {
        TransactionRefT::<Postgres>::new(self.pg_pool.clone())
    }

    async fn commit(transaction: TransactionRefT<Postgres>) {
        transaction
            .into_inner_db_transaction()
            .unwrap()
            .commit()
            .await
            .unwrap();
    }

    fn catalog_for(&self, transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new_chained(&self.base_catalog);
        transaction.clone().into_erased().register(&mut b);
        b.build()
    }

    async fn create_trigger(
        &self,
        transaction: &TransactionRefT<Postgres>,
        flow_type: &str,
    ) -> EventID {
        let flow_binding = FlowBinding::new(flow_type, FlowScope::make_system_scope());
        let event: FlowTriggerEvent = FlowTriggerEventCreated {
            event_time: Utc::now(),
            flow_binding: flow_binding.clone(),
            paused: false,
            rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
                every: chrono::Duration::seconds(5),
            })),
            stop_policy: FlowTriggerStopPolicy::default(),
        }
        .into();

        self.catalog_for(transaction)
            .get_one::<dyn FlowTriggerEventStore>()
            .unwrap()
            .save_events(&flow_binding, None, vec![event])
            .await
            .unwrap()
    }

    async fn has_held_back_events(&self) -> bool {
        let transaction = self.begin();
        let has_held_back_events = {
            let catalog = self.catalog_for(&transaction);
            catalog
                .get_one::<dyn FlowSystemEventBridge>()
                .unwrap()
                .has_held_back_events(&catalog)
                .await
                .unwrap()
        };
        Self::commit(transaction).await;
        has_held_back_events
    }

    /// Projects batches until `count` events have been applied in total
    async fn project_until_count(&self, count: usize) -> Vec<EventID> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut event_ids = Vec::new();
        while event_ids.len() < count {
            assert!(Instant::now() < deadline, "projected only {event_ids:?}");
            event_ids.extend(self.project_next_batch().await);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        event_ids
    }

    /// Fetches the next batch in its own transaction and marks it applied
    async fn project_next_batch(&self) -> Vec<EventID> {
        let transaction = self.begin();
        let events = {
            let catalog = self.catalog_for(&transaction);
            let bridge = catalog.get_one::<dyn FlowSystemEventBridge>().unwrap();

            let events = bridge
                .fetch_next_batch(&catalog, PROJECTOR_NAME, 100)
                .await
                .unwrap();

            let event_ids_with_tx_ids: Vec<_> =
                events.iter().map(|e| (e.event_id, e.tx_id)).collect();
            bridge
                .mark_applied(&catalog, PROJECTOR_NAME, &event_ids_with_tx_ids)
                .await
                .unwrap();

            events
        };
        Self::commit(transaction).await;

        events.into_iter().map(|e| e.event_id).collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
