// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use sqlx::SqlitePool;

use crate::{DatabaseTransactionManager, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteTransactionManager {
    sqlite_pool: SqlitePool,
}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl SqliteTransactionManager {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        Self { sqlite_pool }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for SqliteTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        Ok(TransactionRef::new(self.sqlite_pool.clone()))
    }

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::Sqlite>();
        let maybe_open_sqlite_transaction = transaction_typed.into_inner_db_transaction();
        if let Some(sqlite_transaction) = maybe_open_sqlite_transaction {
            sqlite_transaction.commit().await.int_err()?;
        }

        Ok(())
    }

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::Sqlite>();
        let maybe_open_sqlite_transaction = transaction_typed.into_inner_db_transaction();
        if let Some(sqlite_transaction) = maybe_open_sqlite_transaction {
            sqlite_transaction.rollback().await.int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
