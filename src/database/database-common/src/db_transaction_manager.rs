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
    async fn make_transaction(&self) -> Result<Transaction, InternalError>;

    async fn commit_transaction(&self, transaction: Transaction) -> Result<(), InternalError>;

    async fn rollback_transaction(&self, transaction: Transaction) -> Result<(), InternalError>;
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

    // Wrap transaction into a pointer behind asynchronous mutex
    let transaction_ptr = Arc::new(tokio::sync::Mutex::new(transaction));

    // Create a chained catalog for transaction-aware components,
    // but keep a local copy of transaction pointer
    let chained_catalog = CatalogBuilder::new_chained(base_catalog)
        .add_builder(transaction_ptr.clone())
        .build();

    // Run transactional code in the callback
    let result = callback(chained_catalog).await;

    // Unwrap transaction from pointer and mutex, as catalog is already consumed
    let transaction = Arc::try_unwrap(transaction_ptr).unwrap().into_inner();

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

#[derive(Debug)]
pub struct Transaction {
    pub transaction: Box<dyn Any + Send>,
}

impl Transaction {
    pub fn new<DB: sqlx::Database>(transaction: sqlx::Transaction<'static, DB>) -> Self {
        Self {
            transaction: Box::new(transaction),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
