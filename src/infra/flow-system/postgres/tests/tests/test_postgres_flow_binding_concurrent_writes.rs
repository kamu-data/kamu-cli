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
use kamu_flow_system::*;
use kamu_flow_system_postgres::{
    PostgresFlowConfigurationEventStore,
    PostgresFlowTriggerEventStore,
};
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_trigger_creation_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentTransactionsHarness::new(pg_pool);

    let res = harness.create_trigger_in_racing_transactions().await;

    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_configuration_creation_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentTransactionsHarness::new(pg_pool);

    let res = harness.create_configuration_in_racing_transactions().await;

    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConcurrentTransactionsHarness {
    pg_pool: PgPool,
    flow_binding: FlowBinding,
}

impl ConcurrentTransactionsHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self {
            pg_pool,
            flow_binding: FlowBinding::new("dev.kamu.flow.test", FlowScope::make_system_scope()),
        }
    }

    async fn create_trigger_in_racing_transactions(&self) -> Result<EventID, SaveEventsError> {
        let event: FlowTriggerEvent = FlowTriggerEventCreated {
            event_time: Utc::now(),
            flow_binding: self.flow_binding.clone(),
            paused: false,
            rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
                every: chrono::Duration::seconds(5),
            })),
            stop_policy: FlowTriggerStopPolicy::default(),
        }
        .into();

        let flow_binding = self.flow_binding.clone();
        self.save_in_racing_transactions(move |catalog| {
            let flow_binding = flow_binding.clone();
            let event = event.clone();
            async move {
                catalog
                    .get_one::<dyn FlowTriggerEventStore>()
                    .unwrap()
                    .save_events(&flow_binding, None, vec![event])
                    .await
            }
        })
        .await
    }

    async fn create_configuration_in_racing_transactions(
        &self,
    ) -> Result<EventID, SaveEventsError> {
        let event: FlowConfigurationEvent = FlowConfigurationEventCreated {
            event_time: Utc::now(),
            flow_binding: self.flow_binding.clone(),
            rule: FlowConfigurationRule {
                rule_type: "dev.kamu.flow.test.config".to_string(),
                payload: serde_json::json!({}),
            },
            retry_policy: None,
        }
        .into();

        let flow_binding = self.flow_binding.clone();
        self.save_in_racing_transactions(move |catalog| {
            let flow_binding = flow_binding.clone();
            let event = event.clone();
            async move {
                catalog
                    .get_one::<dyn FlowConfigurationEventStore>()
                    .unwrap()
                    .save_events(&flow_binding, None, vec![event])
                    .await
            }
        })
        .await
    }

    /// Runs `save` in two transactions. The first commits while the second is
    /// in flight; returns the result of the second.
    async fn save_in_racing_transactions<F, Fut>(&self, save: F) -> Result<EventID, SaveEventsError>
    where
        F: Fn(Catalog) -> Fut,
        Fut: Future<Output = Result<EventID, SaveEventsError>> + Send + 'static,
    {
        let first_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let second_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        save(Self::catalog_for(&first_transaction)).await.unwrap();

        let second_save = tokio::spawn(save(Self::catalog_for(&second_transaction)));

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
        b.add::<PostgresFlowTriggerEventStore>();
        b.add::<PostgresFlowConfigurationEventStore>();
        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
