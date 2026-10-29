// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use database_common::TransactionRefT;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts::{Account, AccountRepository};
use kamu_accounts_postgres::PostgresAccountRepository;
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryRepository,
    DatasetReferenceRepository,
    GetDatasetReferenceError,
    SetDatasetReferenceError,
};
use kamu_datasets_postgres::{PostgresDatasetEntryRepository, PostgresDatasetReferenceRepository};
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_update_reference_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentReferenceUpdatesHarness::new(pg_pool);

    let hash_a = odf::Multihash::from_digest_sha3_256(b"a");
    let hash_b = odf::Multihash::from_digest_sha3_256(b"b");
    let hash_c = odf::Multihash::from_digest_sha3_256(b"c");

    harness.create_dataset(Some(&hash_a)).await;

    let res = harness
        .set_in_racing_transactions(Some(&hash_a), &hash_b, &hash_c)
        .await;

    assert_matches!(
        res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.expected_prev_block_hash == Some(hash_a.clone()) &&
           e.actual_prev_block_hash == Some(hash_b.clone())
    );
    assert_eq!(harness.committed_reference().await, Some(hash_b));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_set_initial_reference_in_concurrent_transactions(pg_pool: PgPool) {
    let harness = ConcurrentReferenceUpdatesHarness::new(pg_pool);

    let hash_a = odf::Multihash::from_digest_sha3_256(b"a");
    let hash_b = odf::Multihash::from_digest_sha3_256(b"b");

    harness.create_dataset(None).await;

    let res = harness
        .set_in_racing_transactions(None, &hash_a, &hash_b)
        .await;

    assert_matches!(
        res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.expected_prev_block_hash.is_none() &&
           e.actual_prev_block_hash == Some(hash_a.clone())
    );
    assert_eq!(harness.committed_reference().await, Some(hash_a));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConcurrentReferenceUpdatesHarness {
    pg_pool: PgPool,
    dataset_id: odf::DatasetID,
}

impl ConcurrentReferenceUpdatesHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self {
            pg_pool,
            dataset_id: odf::DatasetID::new_seeded_ed25519(b"racing-dataset"),
        }
    }

    /// Commits a dataset entry, with a `HEAD` reference when `maybe_head` is
    /// given.
    async fn create_dataset(&self, maybe_head: Option<&odf::Multihash>) {
        let transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let catalog = Self::catalog_for(&transaction);

        let account = Account::dummy();
        catalog
            .get_one::<dyn AccountRepository>()
            .unwrap()
            .save_account(&account)
            .await
            .unwrap();

        catalog
            .get_one::<dyn DatasetEntryRepository>()
            .unwrap()
            .save_dataset_entry(&DatasetEntry::new(
                self.dataset_id.clone(),
                account.id,
                account.account_name,
                odf::DatasetName::new_unchecked("racing-dataset"),
                Utc::now(),
                odf::DatasetKind::Root,
            ))
            .await
            .unwrap();

        if let Some(head) = maybe_head {
            catalog
                .get_one::<dyn DatasetReferenceRepository>()
                .unwrap()
                .set_dataset_reference(&self.dataset_id, &odf::BlockRef::Head, None, head)
                .await
                .unwrap();
        }

        drop(catalog);
        Self::commit(transaction).await;
    }

    /// Both transactions move `HEAD` from `expected_head`. The first commits
    /// while the second waits on it; returns the result of the second.
    async fn set_in_racing_transactions(
        &self,
        expected_head: Option<&odf::Multihash>,
        first_head: &odf::Multihash,
        second_head: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let first_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());
        let second_transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        let first_repo = Self::reference_repo_in(&first_transaction);
        first_repo
            .set_dataset_reference(
                &self.dataset_id,
                &odf::BlockRef::Head,
                expected_head,
                first_head,
            )
            .await
            .unwrap();
        drop(first_repo);

        let second_repo = Self::reference_repo_in(&second_transaction);
        let dataset_id = self.dataset_id.clone();
        let expected_head = expected_head.cloned();
        let second_head = second_head.clone();
        let second_set = tokio::spawn(async move {
            second_repo
                .set_dataset_reference(
                    &dataset_id,
                    &odf::BlockRef::Head,
                    expected_head.as_ref(),
                    &second_head,
                )
                .await
        });

        // Let the second write reach the database before the first commits
        tokio::time::sleep(Duration::from_millis(200)).await;

        Self::commit(first_transaction).await;

        second_set.await.unwrap()
    }

    async fn committed_reference(&self) -> Option<odf::Multihash> {
        let transaction = TransactionRefT::<Postgres>::new(self.pg_pool.clone());

        match Self::reference_repo_in(&transaction)
            .get_dataset_reference(&self.dataset_id, &odf::BlockRef::Head)
            .await
        {
            Ok(head) => Some(head),
            Err(GetDatasetReferenceError::NotFound(_)) => None,
            Err(GetDatasetReferenceError::Internal(e)) => panic!("{e}"),
        }
    }

    fn reference_repo_in(
        transaction: &TransactionRefT<Postgres>,
    ) -> Arc<dyn DatasetReferenceRepository> {
        Self::catalog_for(transaction)
            .get_one::<dyn DatasetReferenceRepository>()
            .unwrap()
    }

    fn catalog_for(transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new();
        transaction.clone().into_erased().register(&mut b);
        b.add::<PostgresAccountRepository>();
        b.add::<PostgresDatasetEntryRepository>();
        b.add::<PostgresDatasetReferenceRepository>();
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
