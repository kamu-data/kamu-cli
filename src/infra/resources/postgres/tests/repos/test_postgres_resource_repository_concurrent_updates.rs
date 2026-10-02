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
use event_sourcing::EventID;
use kamu_resources::{
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceRepository,
    ResourceSnapshot,
    TypeUri,
    UpdateResourceError,
};
use kamu_resources_postgres::PostgresResourceRepository;
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
#[ignore = "Reproduces a lost update between concurrent transactions, not fixed yet"]
async fn test_update_resource_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentResourceUpdatesHarness::new(pg_pool);

    let snapshot = harness.create_resource().await;

    let res = harness
        .update_in_racing_transactions(&snapshot, EventID::new(1), EventID::new(2))
        .await;

    assert_matches!(res, Err(UpdateResourceError::ConcurrentModification(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConcurrentResourceUpdatesHarness {
    pg_pool: PgPool,
}

impl ConcurrentResourceUpdatesHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }

    async fn create_resource(&self) -> ResourceSnapshot {
        let transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let repo = Self::repo_in(&transaction);

        let id: ResourceID = repo.new_resource_id().await.unwrap();
        let snapshot = ResourceSnapshot {
            id,
            schema: TypeUri::new_unchecked("TestKind"),
            headers: ResourceHeaders::simple(
                Utc::now(),
                id,
                odf::AccountHandle::new_test("test-account"),
                "racing-resource",
            ),
            spec: serde_json::json!({}),
            status: None,
            last_event_id: None,
        };
        repo.create_resource(&snapshot).await.unwrap();

        drop(repo);
        Self::commit(transaction).await;
        snapshot
    }

    /// Both transactions update `snapshot` expecting its current last event.
    /// The first commits while the second is in flight; returns the result of
    /// the second.
    async fn update_in_racing_transactions(
        &self,
        snapshot: &ResourceSnapshot,
        first_event_id: EventID,
        second_event_id: EventID,
    ) -> Result<(), UpdateResourceError> {
        let first_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let second_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        let expected_last_event_id = snapshot.last_event_id;

        let first_repo = Self::repo_in(&first_transaction);
        let first_update = ResourceSnapshot {
            last_event_id: Some(first_event_id),
            ..snapshot.clone()
        };
        first_repo
            .update_resource(&first_update, expected_last_event_id)
            .await
            .unwrap();
        drop(first_repo);

        let second_repo = Self::repo_in(&second_transaction);
        let second_update = ResourceSnapshot {
            last_event_id: Some(second_event_id),
            ..snapshot.clone()
        };
        let second_save = tokio::spawn(async move {
            second_repo
                .update_resource(&second_update, expected_last_event_id)
                .await
        });

        // Let the second update reach the database before the first commits
        tokio::time::sleep(Duration::from_millis(200)).await;

        Self::commit(first_transaction).await;

        second_save.await.unwrap()
    }

    fn repo_in(transaction: &TransactionRefT<Postgres>) -> std::sync::Arc<dyn ResourceRepository> {
        Self::catalog_for(transaction)
            .get_one::<dyn ResourceRepository>()
            .unwrap()
    }

    fn catalog_for(transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new();
        transaction.clone().into_erased().register(&mut b);
        b.add::<PostgresResourceRepository>();
        b.build()
    }

    async fn commit(transaction: TransactionRefT<Postgres>) {
        transaction
            .into_inner_db_transaction()
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
