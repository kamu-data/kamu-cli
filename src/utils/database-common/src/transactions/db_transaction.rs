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
/// components.
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
        self.maybe_transaction.into_inner()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait TransactionStateErased: std::fmt::Debug + Any + Send + Sync {
    fn register(self: Arc<Self>, catalog_builder: &mut dill::CatalogBuilder);
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
