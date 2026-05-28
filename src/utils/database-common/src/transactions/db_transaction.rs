// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::sync::Arc;

use internal_error::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Cheaply clonable type-erased wrapper that represents a shared reference to
/// [`sqlx::Transaction`] and unifies how we propagate transactions across
/// different database implementations. This reference can appear in multiple
/// components simultaneously, but can be used from only one place at a time via
/// locking. Despite its async nature, this lock must be held only for the
/// duration of the DB query and released when passing control into other
/// components. Transaction is obtained from the connection pool upon first use
/// or when calling [`TransactionRef::begin()`]. For a typical use in
/// repositories, a DB-specific implementation should inject a typed
/// [`TransactionRefT`] instance directly.
#[derive(Debug, Clone)]
pub struct TransactionRef {
    inner: Arc<dyn TransactionStateErased + 'static>,
}

impl TransactionRef {
    pub fn new<DB: sqlx::Database>(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            inner: Arc::new(TransactionState::new(connection_pool)),
        }
    }

    /// Acquires a connection from the connection pool and starts the
    /// transaction if it's not already started. You probably should not call
    /// this - it's only occasionally useful for testing transactions without
    /// downcasting to specific DB type.
    pub async fn begin(&self) -> Result<bool, InternalError> {
        self.inner.begin().await
    }

    /// Performs dynamic downcasting into a DB-specific type. Typically you
    /// should be able to inject [`TransactionRefT`] directly without the need
    /// for manual downcasting.
    pub fn downcast<DB: sqlx::Database>(self) -> TransactionRefT<DB> {
        let inner_any = self.inner as Arc<dyn Any + Send + Sync>;
        let inner_typed = inner_any.downcast::<TransactionState<DB>>().unwrap();
        TransactionRefT { inner: inner_typed }
    }

    /// Use to register both `TransactionRef` and its typed `TransactionRef<DB>`
    /// counterpart in the catalog
    pub fn register(&self, catalog_builder: &mut dill::CatalogBuilder) {
        TransactionStateErased::register(Arc::clone(&self.inner), catalog_builder);
    }
}

impl<DB: sqlx::Database> From<TransactionRefT<DB>> for TransactionRef {
    fn from(value: TransactionRefT<DB>) -> Self {
        value.into_erased()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Cheaply clonable typed wrapper that represents a shared reference to
/// [`sqlx::Transaction`] and unifies how we propagate transactions across
/// different database implementations. This reference can appear in multiple
/// components simultaneously, but can be used from only one place at a time via
/// locking. Despite its async nature, this lock must be held only for the
/// duration of the DB query and released when passing control into other
/// components. Transaction is obtained from the connection pool upon first use
/// or when calling [`TransactionRef::begin()`].
#[derive(Debug)]
pub struct TransactionRefT<DB: sqlx::Database> {
    inner: Arc<TransactionState<DB>>,
}

impl<DB: sqlx::Database> Clone for TransactionRefT<DB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<DB: sqlx::Database> TransactionRefT<DB> {
    pub fn new(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            inner: Arc::new(TransactionState::new(connection_pool)),
        }
    }

    /// Acquires a connection from the connection pool and starts the
    /// transaction if it's not already started. You probably should not call
    /// this - it's only occasionally useful for testing transactions without
    /// downcasting to specific DB type.
    pub async fn begin(&self) -> Result<bool, InternalError> {
        let mut guard = self.lock().await;
        guard.begin().await
    }

    pub async fn lock(&self) -> TransactionGuard<'_, DB> {
        let guard = self.inner.maybe_transaction.lock().await;
        TransactionGuard::new(&self.inner, guard)
    }

    pub fn into_erased(self) -> TransactionRef {
        TransactionRef { inner: self.inner }
    }

    pub fn into_inner_db_transaction(self) -> Option<sqlx::Transaction<'static, DB>> {
        Arc::try_unwrap(self.inner)
            .expect(
                "Attempting to extract inner transaction while more than one strong reference is \
                 present. This may be an indication that transaction reference is leaked, i.e. \
                 held by some component whose lifetime exceeds the intended span of the \
                 transaction scope.",
            )
            .into_inner_db_transaction()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct TransactionState<DB: sqlx::Database> {
    connection_pool: sqlx::pool::Pool<DB>,
    maybe_transaction: tokio::sync::Mutex<Option<sqlx::Transaction<'static, DB>>>,
}

impl<DB: sqlx::Database> TransactionState<DB> {
    fn new(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            connection_pool,
            maybe_transaction: tokio::sync::Mutex::new(None),
        }
    }

    fn into_inner_db_transaction(self) -> Option<sqlx::Transaction<'static, DB>> {
        let mut guard = self.maybe_transaction.try_lock().expect(
            "into_inner_db_transaction will always be called when no other references to \
             transaction exist and the lock can be acquired without blocking",
        );
        guard.take()
    }
}

impl<DB: sqlx::Database> Drop for TransactionState<DB> {
    fn drop(&mut self) {
        match self.maybe_transaction.try_lock() {
            Ok(guard) if guard.is_none() => (),
            // This log message compliments COMMIT / ROLLBACK logs we have in
            // `DatabaseTransactionRunner` and is used to detect when transaction gets
            // dropped without being committted or rolled back
            _ => tracing::warn!("Transaction DROPPED"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait TransactionStateErased: std::fmt::Debug + Any + Send + Sync {
    fn register(self: Arc<Self>, catalog_builder: &mut dill::CatalogBuilder);

    /// Acquires a connection from the connection pool and starts the
    /// transaction if it's not already started. You probably should not call
    /// this - it's only occasionally useful for testing transactions without
    /// downcasting to specific DB type.
    async fn begin(&self) -> Result<bool, InternalError>;
}

#[async_trait::async_trait]
impl<DB: sqlx::Database> TransactionStateErased for TransactionState<DB> {
    fn register(self: Arc<Self>, catalog_builder: &mut dill::CatalogBuilder) {
        // Add typed wrapper
        catalog_builder.add_value(TransactionRefT {
            inner: Arc::clone(&self),
        });

        // Add erased wrapper
        catalog_builder.add_value(TransactionRef { inner: self });
    }

    async fn begin(&self) -> Result<bool, InternalError> {
        let guard = self.maybe_transaction.lock().await;
        let mut guard = TransactionGuard::new(self, guard);
        guard.begin().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a lock held over shared [`TransactionRef`] that allows to safely
/// access typed [`sqlx::Transaction`] object. Despite its async nature, this
/// lock must be held only for the duration of the DB query and released when
/// passing control into other components.
pub struct TransactionGuard<'a, DB: sqlx::Database> {
    state: &'a TransactionState<DB>,
    guard: tokio::sync::MutexGuard<'a, Option<sqlx::Transaction<'static, DB>>>,
}

impl<'a, DB: sqlx::Database> TransactionGuard<'a, DB> {
    fn new(
        state: &'a TransactionState<DB>,
        guard: tokio::sync::MutexGuard<'a, Option<sqlx::Transaction<'static, DB>>>,
    ) -> Self {
        Self { state, guard }
    }

    /// Acquires a connection from the connection pool and starts the
    /// transaction if it's not already started
    pub async fn begin(&mut self) -> Result<bool, InternalError> {
        if self.guard.is_some() {
            return Ok(false);
        }

        tracing::debug!("Opening transaction");
        let transaction = self.state.connection_pool.begin().await.int_err()?;
        (*self.guard) = Some(transaction);

        Ok(true)
    }

    pub async fn connection_mut(&mut self) -> Result<&mut DB::Connection, InternalError> {
        self.begin().await?;
        let tx = self.guard.as_deref_mut().unwrap();
        Ok(tx)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
