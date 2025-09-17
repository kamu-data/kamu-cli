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
use sqlx::PgPool;

use crate::{DatabaseTransactionManager, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresTransactionManager {
    pg_pool: PgPool,
}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl PostgresTransactionManager {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for PostgresTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        Ok(TransactionRef::new(self.pg_pool.clone()))
    }

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::Postgres>();
        let maybe_open_postgres_transaction = transaction_typed.into_maybe_transaction();
        if let Some(postgres_transaction) = maybe_open_postgres_transaction {
            postgres_transaction.commit().await.int_err()?;
        }

        Ok(())
    }

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::Postgres>();
        let maybe_open_postgres_transaction = transaction_typed.into_maybe_transaction();
        if let Some(postgres_transaction) = maybe_open_postgres_transaction {
            postgres_transaction.rollback().await.int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
