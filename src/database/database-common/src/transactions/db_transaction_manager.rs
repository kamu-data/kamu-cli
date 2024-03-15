// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use tokio::sync::Mutex;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatabaseTransactionManager: Send + Sync {
    async fn make_transaction(&self) -> Result<TransactionRef, InternalError>;

    async fn commit_transaction(&self, transaction: TransactionRef) -> Result<(), InternalError>;

    async fn rollback_transaction(&self, transaction: TransactionRef) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn run_transactional<H, HFut>(
    base_catalog: &Catalog,
    callback: H,
) -> Result<(), InternalError>
where
    H: FnOnce(Catalog) -> HFut + Send + Sync + 'static,
    HFut: std::future::Future<Output = Result<(), InternalError>> + Send + 'static,
{
    // Extract transaction manager, specific for the database
    let db_transaction_manager = base_catalog
        .get_one::<dyn DatabaseTransactionManager>()
        .unwrap();

    // Start transaction
    let transaction = db_transaction_manager.make_transaction().await?;

    // Create a chained catalog for transaction-aware components,
    // but keep a local copy of transaction pointer
    let chained_catalog = CatalogBuilder::new_chained(base_catalog)
        .add_value(transaction.clone())
        .build();

    // Run transactional code in the callback
    let result = callback(chained_catalog).await;

    // Commit or rollback transaction depending on the result
    match result {
        // In case everything succeeded, commit the transaction
        Ok(_) => {
            db_transaction_manager
                .commit_transaction(transaction)
                .await?;
            Ok(())
        }

        // Otherwise, do an explicit rollback
        Err(e) => {
            db_transaction_manager
                .rollback_transaction(transaction)
                .await?;
            Err(e)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents a shared reference to [`sqlx::Transaction`] that unifies how we
/// propagate transactions across different database implementations. This
/// reference can appear in multiple components simultaneously, but can be used
/// from only one place at a time via locking. Despite its async nature, this
/// lock must be held only for the duration of the DB query and released when
/// passing control into other components.
#[derive(Clone)]
pub struct TransactionRef {
    p: Arc<Mutex<Box<dyn Any + Send>>>,
}

impl TransactionRef {
    pub fn new<DB: sqlx::Database>(transaction: sqlx::Transaction<'static, DB>) -> Self {
        Self {
            p: Arc::new(Mutex::new(Box::new(transaction))),
        }
    }

    /// Extracts the inner [`sqlx::Transaction`]. Will panic if there are any
    /// outstanding strong reference held.
    pub fn into_inner<DB: sqlx::Database>(self) -> sqlx::Transaction<'static, DB> {
        let m = Arc::try_unwrap(self.p).unwrap().into_inner();
        *m.downcast::<sqlx::Transaction<'static, DB>>().unwrap()
    }
}

/// A typed wrapper over the [`TransactionRef`]. It propagates the type
/// information to [`TransactionGuard`] to safely access typed
/// [`sqlx::Transaction`] object.
pub struct TransactionRefT<DB: sqlx::Database> {
    tr: TransactionRef,
    _phantom: PhantomData<DB>,
}

impl<DB: sqlx::Database> TransactionRefT<DB> {
    pub fn new(tr: TransactionRef) -> Self {
        Self {
            tr,
            _phantom: PhantomData,
        }
    }

    pub async fn lock(&self) -> TransactionGuard<'_, DB> {
        TransactionGuard::new(self.tr.p.lock().await)
    }
}

impl<DB: sqlx::Database> From<TransactionRef> for TransactionRefT<DB> {
    fn from(value: TransactionRef) -> Self {
        Self::new(value)
    }
}

/// Represents a lock held over shared [`TransactionRef`] that allows to safely
/// access typed [`sqlx::Transaction`] object. Despite its async nature, this
/// lock must be held only for the duration of the DB query and released when
/// passing control into other components.
pub struct TransactionGuard<'a, DB: sqlx::Database> {
    guard: tokio::sync::MutexGuard<'a, Box<dyn Any + Send>>,
    _phantom: PhantomData<DB>,
}

impl<'a, DB: sqlx::Database> TransactionGuard<'a, DB> {
    pub fn new(guard: tokio::sync::MutexGuard<'a, Box<dyn Any + Send>>) -> Self {
        Self {
            guard,
            _phantom: PhantomData,
        }
    }

    pub fn connection_mut(&mut self) -> &mut DB::Connection {
        self.guard
            .downcast_mut::<sqlx::Transaction<'static, DB>>()
            .unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
