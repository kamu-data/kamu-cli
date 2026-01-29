// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};

use crate::TransactionRef;

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

#[dill::component]
pub struct DatabaseTransactionRunner {
    catalog: dill::Catalog,
}

impl DatabaseTransactionRunner {
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
        H: FnOnce(dill::Catalog) -> HFut,
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
            let mut b = self.catalog.builder_chained();
            transaction_ref.register(&mut b);

            let catalog_with_transaction = b.build();

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
        self.transactional(|transaction_catalog| async move {
            let catalog_item = transaction_catalog.get_one().int_err()?;

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
        self.transactional(|transaction_catalog| async move {
            let catalog_item1 = transaction_catalog.get_one().int_err()?;
            let catalog_item2 = transaction_catalog.get_one().int_err()?;

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
        self.transactional(|transaction_catalog| async move {
            let catalog_item1 = transaction_catalog.get_one().int_err()?;
            let catalog_item2 = transaction_catalog.get_one().int_err()?;
            let catalog_item3 = transaction_catalog.get_one().int_err()?;

            callback(catalog_item1, catalog_item2, catalog_item3).await
        })
        .await
    }

    pub async fn transactional_with4<
        Iface1,
        Iface2,
        Iface3,
        Iface4,
        H,
        HFut,
        HFutResultT,
        HFutResultE,
    >(
        &self,
        callback: H,
    ) -> Result<HFutResultT, HFutResultE>
    where
        Iface1: 'static + ?Sized + Send + Sync,
        Iface2: 'static + ?Sized + Send + Sync,
        Iface3: 'static + ?Sized + Send + Sync,
        Iface4: 'static + ?Sized + Send + Sync,
        H: FnOnce(Arc<Iface1>, Arc<Iface2>, Arc<Iface3>, Arc<Iface4>) -> HFut,
        HFut: std::future::Future<Output = Result<HFutResultT, HFutResultE>>,
        HFutResultE: From<InternalError>,
    {
        self.transactional(|transaction_catalog| async move {
            let catalog_item1 = transaction_catalog.get_one().int_err()?;
            let catalog_item2 = transaction_catalog.get_one().int_err()?;
            let catalog_item3 = transaction_catalog.get_one().int_err()?;
            let catalog_item4 = transaction_catalog.get_one().int_err()?;

            callback(catalog_item1, catalog_item2, catalog_item3, catalog_item4).await
        })
        .await
    }
}

impl From<dill::Catalog> for DatabaseTransactionRunner {
    fn from(value: dill::Catalog) -> Self {
        Self::new(value)
    }
}

impl From<dill::CatalogWeakRef> for DatabaseTransactionRunner {
    fn from(value: dill::CatalogWeakRef) -> Self {
        Self::new(value.upgrade())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
