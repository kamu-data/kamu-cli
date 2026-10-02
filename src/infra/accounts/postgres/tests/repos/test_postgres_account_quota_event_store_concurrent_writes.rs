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
use event_sourcing::{EventID, SaveEventsError};
use kamu_accounts::{
    AccountQuotaAdded,
    AccountQuotaEvent,
    AccountQuotaEventStore,
    AccountQuotaPayload,
    AccountQuotaQuery,
    QuotaType,
    QuotaUnit,
};
use kamu_accounts_postgres::PostgresAccountQuotaEventStore;
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
#[ignore = "Reproduces a lost update between concurrent transactions, not fixed yet"]
async fn test_quota_creation_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentQuotaWritesHarness::new(pg_pool);

    let res = harness.add_quota_in_racing_transactions().await;

    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConcurrentQuotaWritesHarness {
    pg_pool: PgPool,
    query: AccountQuotaQuery,
}

impl ConcurrentQuotaWritesHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self {
            pg_pool,
            query: AccountQuotaQuery {
                account_id: odf::AccountID::new_seeded_ed25519(b"racing-quota-user"),
                quota_type: QuotaType::storage_space(),
            },
        }
    }

    fn quota_added(&self) -> AccountQuotaEvent {
        AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
            event_time: Utc::now(),
            quota_id: uuid::Uuid::new_v4(),
            account_id: self.query.account_id.clone(),
            quota_type: self.query.quota_type.clone(),
            quota_payload: AccountQuotaPayload {
                units: QuotaUnit::Bytes,
                value: 10,
            },
        })
    }

    /// Both transactions add the quota as its first event. The first commits
    /// while the second is in flight; returns the result of the second.
    async fn add_quota_in_racing_transactions(&self) -> Result<EventID, SaveEventsError> {
        let first_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let second_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        Self::store_in(&first_transaction)
            .save_events(&self.query, None, vec![self.quota_added()])
            .await
            .unwrap();

        let second_store = Self::store_in(&second_transaction);
        let query = self.query.clone();
        let event = self.quota_added();
        let second_save =
            tokio::spawn(async move { second_store.save_events(&query, None, vec![event]).await });

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

    fn store_in(
        transaction: &TransactionRefT<Postgres>,
    ) -> std::sync::Arc<dyn AccountQuotaEventStore> {
        Self::catalog_for(transaction)
            .get_one::<dyn AccountQuotaEventStore>()
            .unwrap()
    }

    fn catalog_for(transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new();
        transaction.clone().into_erased().register(&mut b);
        b.add::<PostgresAccountQuotaEventStore>();
        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
