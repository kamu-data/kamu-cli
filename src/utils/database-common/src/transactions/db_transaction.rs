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
/// components.
#[derive(Clone)]
pub struct TransactionRef {
    inner: Arc<dyn TransactionStateErased + 'static>,
}

impl TransactionRef {
    pub fn new<DB: sqlx::Database>(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            inner: Arc::new(TransactionState::new(connection_pool)),
        }
    }

    pub fn into_maybe_transaction<DB: sqlx::Database>(
        self,
    ) -> Option<sqlx::Transaction<'static, DB>> {
        let inner_any = self.inner as Arc<dyn Any + Send + Sync>;
        let inner_typed = inner_any.downcast::<TransactionState<DB>>().unwrap();

        Arc::try_unwrap(inner_typed)
            .expect(
                "Attempting to extract inner transaction while more than one strong reference is \
                 present. This may be an indication that transaction reference is leaked, i.e. \
                 held by some component whose lifetime exceeds the intended span of the \
                 transaction scope.",
            )
            .into_maybe_transaction()
    }

    pub fn downcast<DB: sqlx::Database>(self) -> TransactionRefT<DB> {
        let inner_any = self.inner as Arc<dyn Any + Send + Sync>;
        let inner_typed = inner_any.downcast::<TransactionState<DB>>().unwrap();
        TransactionRefT { inner: inner_typed }
    }

    #[deprecated = "Do not use, this is a temporary hack"]
    pub async fn commit(&self) -> Result<(), InternalError> {
        self.inner.commit().await
    }
}

// TODO: Replace with registering a typed wrapper in catalog and injecting
// directly
impl<DB: sqlx::Database> From<TransactionRef> for TransactionRefT<DB> {
    fn from(value: TransactionRef) -> Self {
        value.downcast::<DB>()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Cheaply clonable typed wrapper that represents a shared reference to
/// [`sqlx::Transaction`] and unifies how we propagate transactions across
/// different database implementations. This reference can appear in multiple
/// components simultaneously, but can be used from only one place at a time via
/// locking. Despite its async nature, this lock must be held only for the
/// duration of the DB query and released when passing control into other
/// components.
#[derive(Debug)]
pub struct TransactionRefT<DB: sqlx::Database> {
    inner: Arc<TransactionState<DB>>,
}

impl<DB: sqlx::Database> TransactionRefT<DB> {
    pub fn new(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            inner: Arc::new(TransactionState::new(connection_pool)),
        }
    }

    pub async fn lock(&self) -> TransactionGuard<'_, DB> {
        let guard = self.inner.maybe_transaction.lock().await;
        TransactionGuard::new(&self.inner, guard)
    }

    pub fn into_maybe_transaction(self) -> Option<sqlx::Transaction<'static, DB>> {
        Arc::try_unwrap(self.inner)
            .expect(
                "Attempting to extract inner transaction while more than one strong reference is \
                 present. This may be an indication that transaction reference is leaked, i.e. \
                 held by some component whose lifetime exceeds the intended span of the \
                 transaction scope.",
            )
            .into_maybe_transaction()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait TransactionStateErased: Any + Send + Sync {
    async fn commit(&self) -> Result<(), InternalError>;
}

#[async_trait::async_trait]
impl<DB: sqlx::Database> TransactionStateErased for TransactionState<DB> {
    async fn commit(&self) -> Result<(), InternalError> {
        TransactionState::commit(self).await
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

    async fn commit(&self) -> Result<(), InternalError> {
        let mut guard = self.maybe_transaction.lock().await;

        if let Some(tx) = guard.take() {
            tracing::warn!("Force-committing an ongoing transaction");
            tx.commit().await.int_err()
        } else {
            tracing::error!("Transaction was not open");
            Ok(())
        }
    }

    fn into_maybe_transaction(self) -> Option<sqlx::Transaction<'static, DB>> {
        self.maybe_transaction.into_inner()
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

    pub async fn connection_mut(&mut self) -> Result<&mut DB::Connection, InternalError> {
        if self.guard.is_none() {
            tracing::warn!("Opening transaction");
            let transaction = self.state.connection_pool.begin().await.int_err()?;
            (*self.guard) = Some(transaction);
        }

        let tx = self.guard.as_deref_mut().unwrap();
        Ok(tx)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
