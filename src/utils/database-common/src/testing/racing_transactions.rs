// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::time::Duration;

use dill::Catalog;
use internal_error::InternalError;

use crate::{DatabaseTransactionManager, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How long the second transaction gets to reach the storage before the first
/// commits, so a database makes it wait on the first's locks
const SECOND_WRITE_HEAD_START: Duration = Duration::from_millis(200);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What happened to the second of two racing transactions
#[derive(Debug)]
pub enum RacingTransactionOutcome<E> {
    /// Its write returned an error; the transaction was rolled back
    FailedAtWrite(E),
    /// Its write succeeded, but its commit failed
    FailedAtCommit(InternalError),
    /// It committed
    Committed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs `second_write` in a transaction that starts while the transaction of
/// `first_write` is open, and commits the first while the second is writing.
/// The second commits only after the first, so the outcome is the same on every
/// storage that detects the conflict: a database fails the second write once
/// the first commits, the in-memory manager fails the second commit.
///
/// `catalog` must provide a `DatabaseTransactionManager`; `first_write` must
/// succeed. Both writes must drop everything they resolve from their catalog.
pub async fn race_transactions<F1, Fut1, F2, Fut2, E>(
    catalog: &Catalog,
    first_write: F1,
    second_write: F2,
) -> RacingTransactionOutcome<E>
where
    F1: FnOnce(Catalog) -> Fut1,
    Fut1: Future<Output = ()>,
    F2: FnOnce(Catalog) -> Fut2 + Send + 'static,
    Fut2: Future<Output = Result<(), E>> + Send,
    E: Send + 'static,
{
    let transaction_manager = catalog.get_one::<dyn DatabaseTransactionManager>().unwrap();

    let first = transaction_manager.make_transaction_ref().await.unwrap();
    first_write(catalog_with(catalog, &first)).await;

    // Begun inside the task: storage with one connection makes it wait for the
    // first to finish
    let second = tokio::spawn({
        let catalog = catalog.clone();
        let transaction_manager = transaction_manager.clone();
        async move {
            let second = transaction_manager.make_transaction_ref().await.unwrap();
            let result = second_write(catalog_with(&catalog, &second)).await;
            (second, result)
        }
    });

    tokio::time::sleep(SECOND_WRITE_HEAD_START).await;
    transaction_manager.commit_transaction(first).await.unwrap();

    let (second, result) = second.await.unwrap();
    match result {
        Err(e) => {
            transaction_manager
                .rollback_transaction(second)
                .await
                .unwrap();
            RacingTransactionOutcome::FailedAtWrite(e)
        }
        Ok(()) => match transaction_manager.commit_transaction(second).await {
            Err(e) => RacingTransactionOutcome::FailedAtCommit(e),
            Ok(()) => RacingTransactionOutcome::Committed,
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn catalog_with(catalog: &Catalog, transaction: &TransactionRef) -> Catalog {
    let mut b = catalog.builder_chained();
    transaction.register(&mut b);
    b.build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
