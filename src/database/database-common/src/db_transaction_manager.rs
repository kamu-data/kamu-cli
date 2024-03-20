// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

use dill::Catalog;
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
    db_transaction_manager: &dyn DatabaseTransactionManager,
    base_catalog: Catalog,
    callback: H,
) -> Result<(), InternalError>
where
    H: FnOnce(Catalog, TransactionSubject) -> HFut + Send + Sync + 'static,
    HFut: std::future::Future<Output = Result<TransactionSubject, InternalError>> + Send + 'static,
{
    // Start transaction
    let transaction_subject = db_transaction_manager
        .make_transaction_subject(&base_catalog)
        .await?;

    // Run transactional code in the callback
    // Note: in case of error, the transaction rolls back automatically
    let transaction_subject = callback(base_catalog, transaction_subject).await?;

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
