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

use dill::{Catalog, CatalogBuilder, component};
use internal_error::{InternalError, ResultIntoInternal};
use tokio::sync::Mutex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatabaseTransactionManager: Send + Sync {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError>;

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError>;

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseTransactionRunner {
    catalog: Catalog,
}

#[component(pub)]
impl DatabaseTransactionRunner {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    #[tracing::instrument(
        name = "DatabaseTransactionRunner::transactional",
        level = "debug",
        skip_all
    )]
    pub async fn transactional<H, HFut, HFutResultT, HFutResultE>(
        &self,
        callback: H,
    ) -> Result<HFutResultT, HFutResultE>
    where
        H: FnOnce(Catalog) -> HFut,
        HFut: std::future::Future<Output = Result<HFutResultT, HFutResultE>>,
        HFutResultE: From<InternalError>,
    {
        use tracing::Instrument;

        // Extract transaction manager, specific for the database
        let db_transaction_manager = self
            .catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .unwrap();

        // Start transaction
        let transaction_ref = db_transaction_manager.make_transaction_ref().await?;

        // A catalog with a transaction must live for a limited time
        let result = {
            // Create a chained catalog for transaction-aware components,
            // but keep a local copy of a transaction pointer
            let catalog_with_transaction = CatalogBuilder::new_chained(&self.catalog)
                .add_value(transaction_ref.clone())
                .build();

            callback(catalog_with_transaction)
                .instrument(tracing::trace_span!(
                    "DatabaseTransactionRunner::transactional::callback"
                ))
                .await
        };

        // Commit or rollback transaction depending on the result
        match result {
            // In case everything succeeded, commit the transaction
            Ok(res) => {
                tracing::debug!("Transaction COMMIT");

                db_transaction_manager
                    .commit_transaction(transaction_ref)
                    .await?;

                Ok(res)
            }

            // Otherwise, do an explicit rollback
            Err(e) => {
                tracing::warn!("Transaction ROLLBACK");

                db_transaction_manager
                    .rollback_transaction(transaction_ref)
                    .await?;

                Err(e)
            }
        }
    }

    pub async fn transactional_with<Iface, H, HFut, HFutResultT, HFutResultE>(
        &self,
        callback: H,
    ) -> Result<HFutResultT, HFutResultE>
    where
        Iface: 'static + ?Sized + Send + Sync,
        H: FnOnce(Arc<Iface>) -> HFut,
        HFut: std::future::Future<Output = Result<HFutResultT, HFutResultE>>,
        HFutResultE: From<InternalError>,
    {
        self.transactional(|transactional_catalog| async move {
            let catalog_item = transactional_catalog.get_one().int_err()?;

            callback(catalog_item).await
        })
        .await
    }

    pub async fn transactional_with2<Iface1, Iface2, H, HFut, HFutResultT, HFutResultE>(
        &self,
        callback: H,
    ) -> Result<HFutResultT, HFutResultE>
    where
        Iface1: 'static + ?Sized + Send + Sync,
        Iface2: 'static + ?Sized + Send + Sync,
        H: FnOnce(Arc<Iface1>, Arc<Iface2>) -> HFut,
        HFut: std::future::Future<Output = Result<HFutResultT, HFutResultE>>,
        HFutResultE: From<InternalError>,
    {
        self.transactional(|transactional_catalog| async move {
            let catalog_item1 = transactional_catalog.get_one().int_err()?;
            let catalog_item2 = transactional_catalog.get_one().int_err()?;

            callback(catalog_item1, catalog_item2).await
        })
        .await
    }

    pub async fn transactional_with3<Iface1, Iface2, Iface3, H, HFut, HFutResultT, HFutResultE>(
        &self,
        callback: H,
    ) -> Result<HFutResultT, HFutResultE>
    where
        Iface1: 'static + ?Sized + Send + Sync,
        Iface2: 'static + ?Sized + Send + Sync,
        Iface3: 'static + ?Sized + Send + Sync,
        H: FnOnce(Arc<Iface1>, Arc<Iface2>, Arc<Iface3>) -> HFut,
        HFut: std::future::Future<Output = Result<HFutResultT, HFutResultE>>,
        HFutResultE: From<InternalError>,
    {
        self.transactional(|transactional_catalog| async move {
            let catalog_item1 = transactional_catalog.get_one().int_err()?;
            let catalog_item2 = transactional_catalog.get_one().int_err()?;
            let catalog_item3 = transactional_catalog.get_one().int_err()?;

            callback(catalog_item1, catalog_item2, catalog_item3).await
        })
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a shared reference to [`sqlx::Transaction`] that unifies how we
/// propagate transactions across different database implementations. This
/// reference can appear in multiple components simultaneously, but can be used
/// from only one place at a time via locking. Despite its async nature, this
/// lock must be held only for the duration of the DB query and released when
/// passing control into other components.
#[derive(Clone)]
pub struct TransactionRef {
    inner: Arc<Mutex<TransactionRefInner>>,
}

impl TransactionRef {
    pub fn new<DB: sqlx::Database>(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionRefInner::new(connection_pool))),
        }
    }

    pub fn into_maybe_transaction<DB: sqlx::Database>(
        self,
    ) -> Option<sqlx::Transaction<'static, DB>> {
        let inner = Arc::try_unwrap(self.inner)
            .expect(
                "Attempting to extract inner transaction while more than one strong reference is \
                 present. This may be an indication that transaction reference is leaked, i.e. \
                 held by some component whose lifetime exceeds the intended span of the \
                 transaction scope.",
            )
            .into_inner();

        inner
            .maybe_transaction
            .map(|t| *t.downcast::<sqlx::Transaction<'static, DB>>().unwrap())
    }
}

#[derive(Debug)]
pub struct TransactionRefInner {
    connection_pool: Box<dyn Any + Send>,
    maybe_transaction: Option<Box<dyn Any + Send>>,
}

impl TransactionRefInner {
    fn new<DB: sqlx::Database>(connection_pool: sqlx::pool::Pool<DB>) -> Self {
        Self {
            connection_pool: Box::new(connection_pool),
            maybe_transaction: None,
        }
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
        TransactionGuard::new(self.tr.inner.lock().await)
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
    guard: tokio::sync::MutexGuard<'a, TransactionRefInner>,
    _phantom: PhantomData<DB>,
}

impl<'a, DB: sqlx::Database> TransactionGuard<'a, DB> {
    pub fn new(guard: tokio::sync::MutexGuard<'a, TransactionRefInner>) -> Self {
        Self {
            guard,
            _phantom: PhantomData,
        }
    }

    pub async fn connection_mut(&mut self) -> Result<&mut DB::Connection, InternalError> {
        if self.guard.maybe_transaction.is_none() {
            let pool = self
                .guard
                .connection_pool
                .downcast_mut::<sqlx::pool::Pool<DB>>()
                .unwrap();
            let transaction = pool.begin().await.int_err()?;
            self.guard.maybe_transaction = Some(Box::new(transaction));
        }

        let transaction = self.guard.maybe_transaction.as_deref_mut().unwrap();
        Ok(transaction
            .downcast_mut::<sqlx::Transaction<'static, DB>>()
            .unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
