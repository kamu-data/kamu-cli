// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{TransactionRef, TransactionScope};

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

        let transaction = TransactionScope::begin(&self.catalog, true).await?;

        let result = callback(transaction.catalog().clone())
            .instrument(tracing::trace_span!(
                "DatabaseTransactionRunner::transactional::callback"
            ))
            .await;

        transaction.finish(result).await
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
