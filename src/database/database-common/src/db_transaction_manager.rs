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

use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatabaseTransactionManager: Send + Sync {
    async fn make_transaction_subject(
        &self,
        base_catalog: &Catalog,
    ) -> Result<TransactionSubject, InternalError>;

    async fn commit_transaction(
        &self,
        transaction_subject: TransactionSubject,
    ) -> Result<(), InternalError>;

    async fn rollback_transaction(
        &self,
        transaction_subject: TransactionSubject,
    ) -> Result<(), InternalError>;
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
    let transaction_subject = db_transaction_manager
        .make_transaction_subject(base_catalog)
        .await?;

    // Wrap transaction into a pointer behind asynchronous mutex
    let transaction_subject_ptr = Arc::new(tokio::sync::Mutex::new(transaction_subject));

    // Create a chained catalog for transaction-aware components, but keep a local
    // copy of transaction pointer
    let chained_catalog = CatalogBuilder::new_chained(base_catalog)
        .add_value(transaction_subject_ptr.clone())
        .build();

    // Run transactional code in the callback
    // Note: in case of error, the transaction rolls back automatically
    callback(chained_catalog).await?;

    // Unwrap transaction subject from pointer and mutex, as catalog is already
    // consumed
    let transaction_subject = Arc::try_unwrap(transaction_subject_ptr)
        .unwrap()
        .into_inner();

    // Commit transaction
    db_transaction_manager
        .commit_transaction(transaction_subject)
        .await?;

    // Success
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TransactionSubject {
    pub transaction: Box<dyn Any>,
}

impl TransactionSubject {
    pub fn new<DB: sqlx::Database>(transaction: sqlx::Transaction<'static, DB>) -> Self {
        Self {
            transaction: Box::new(transaction),
        }
    }
}

unsafe impl Send for TransactionSubject {}

unsafe impl Sync for TransactionSubject {}

/////////////////////////////////////////////////////////////////////////////////////////
