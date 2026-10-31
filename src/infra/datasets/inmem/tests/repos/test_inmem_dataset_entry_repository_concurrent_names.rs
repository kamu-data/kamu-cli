// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use database_common::{DatabaseTransactionManager, TransactionRef};
use dill::CatalogBuilder;
use internal_error::InternalError;
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};

use super::InMemoryDatasetRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_same_name_in_concurrent_transactions() {
    let harness = ConcurrentNamesHarness::new();
    let first = harness.begin().await;
    let second = harness.begin().await;

    harness.save_entry(&first, b"first", "foo").await;
    harness.save_entry(&second, b"second", "Foo").await;

    harness.commit(first).await.unwrap();
    assert!(harness.commit(second).await.is_err());
    pretty_assertions::assert_eq!(
        harness.committed_dataset_named("foo").await,
        Some(odf::DatasetID::new_seeded_ed25519(b"first"))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Entries of one owner, saved in transactions of the in-memory manager
struct ConcurrentNamesHarness {
    base: InMemoryDatasetRepositoriesHarness,
    owner_id: odf::AccountID,
}

impl ConcurrentNamesHarness {
    fn new() -> Self {
        Self {
            base: InMemoryDatasetRepositoriesHarness::new_transactional(),
            owner_id: odf::AccountID::new_seeded_ed25519(b"owner"),
        }
    }

    async fn begin(&self) -> TransactionRef {
        self.transaction_manager()
            .make_transaction_ref()
            .await
            .unwrap()
    }

    async fn commit(&self, transaction: TransactionRef) -> Result<(), InternalError> {
        self.transaction_manager()
            .commit_transaction(transaction)
            .await
    }

    async fn save_entry(&self, transaction: &TransactionRef, id_seed: &[u8], name: &str) {
        self.repository(Some(transaction))
            .save_dataset_entry(&DatasetEntry::new(
                odf::DatasetID::new_seeded_ed25519(id_seed),
                self.owner_id.clone(),
                odf::AccountName::new_unchecked("owner"),
                odf::DatasetName::new_unchecked(name),
                Utc::now(),
                odf::DatasetKind::Root,
            ))
            .await
            .unwrap();
    }

    async fn committed_dataset_named(&self, name: &str) -> Option<odf::DatasetID> {
        self.repository(None)
            .get_dataset_entry_by_owner_and_name(
                &self.owner_id,
                &odf::DatasetName::new_unchecked(name),
            )
            .await
            .ok()
            .map(|entry| entry.id)
    }

    fn repository(&self, transaction: Option<&TransactionRef>) -> Arc<dyn DatasetEntryRepository> {
        let mut b = CatalogBuilder::new_chained(&self.base.catalog);
        if let Some(transaction) = transaction {
            b.add_value(transaction.clone());
        }
        b.build().get_one().unwrap()
    }

    fn transaction_manager(&self) -> Arc<dyn DatabaseTransactionManager> {
        self.base.catalog.get_one().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
