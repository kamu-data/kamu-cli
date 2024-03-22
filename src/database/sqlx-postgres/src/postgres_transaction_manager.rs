// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionManager, Transaction};
use dill::*;
use kamu_core::{InternalError, ResultIntoInternal};
use sqlx::PgPool;

/////////////////////////////////////////////////////////////////////////////////////////

pub type PostgresTransaction = sqlx::Transaction<'static, sqlx::Postgres>;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresTransactionManager {
    pg_pool: Arc<PgPool>,
}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl PostgresTransactionManager {
    pub fn new(pg_pool: Arc<PgPool>) -> Self {
        Self { pg_pool }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for PostgresTransactionManager {
    async fn make_transaction(&self) -> Result<Transaction, InternalError> {
        let postgres_transaction = self.pg_pool.begin().await.int_err()?;
        Ok(Transaction::new(postgres_transaction))
    }

    async fn commit_transaction(&self, transaction: Transaction) -> Result<(), InternalError> {
        let postgres_transaction = transaction
            .transaction
            .downcast::<PostgresTransaction>()
            .unwrap();

        postgres_transaction.commit().await.int_err()?;
        Ok(())
    }

    async fn rollback_transaction(&self, transaction: Transaction) -> Result<(), InternalError> {
        let postgres_transaction = transaction
            .transaction
            .downcast::<PostgresTransaction>()
            .unwrap();

        postgres_transaction.rollback().await.int_err()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
