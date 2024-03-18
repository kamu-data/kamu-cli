// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::{Borrow, BorrowMut};

use database_common::{
    DatabaseConfiguration,
    DatabaseConnectionPoolError,
    DatabaseTransactionError,
};
use sqlx::{PgPool, Postgres, Transaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresConnectionPool {
    pg_pool: PgPool,
}

impl PostgresConnectionPool {
    pub fn new(
        db_configuration: &DatabaseConfiguration,
    ) -> Result<Self, DatabaseConnectionPoolError> {
        Ok(Self {
            pg_pool: PgPool::connect_lazy(db_configuration.connection_string().as_str())
                .map_err(DatabaseConnectionPoolError::SqlxError)?,
        })
    }

    pub async fn begin_transaction<'c>(
        &self,
    ) -> Result<PostgresConnectionPoolTransaction<'c>, DatabaseConnectionPoolError> {
        let pg_transaction = self
            .pg_pool
            .begin()
            .await
            .map_err(DatabaseConnectionPoolError::SqlxError)?;

        Ok(PostgresConnectionPoolTransaction(pg_transaction))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresConnectionPoolTransaction<'c>(Transaction<'c, Postgres>);

impl<'c> std::ops::Deref for PostgresConnectionPoolTransaction<'c> {
    type Target = Transaction<'c, Postgres>;

    fn deref(&self) -> &Self::Target {
        self.0.borrow()
    }
}

impl<'c> std::ops::DerefMut for PostgresConnectionPoolTransaction<'c> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.borrow_mut()
    }
}

impl<'c> PostgresConnectionPoolTransaction<'c> {
    pub async fn commit(self) -> Result<(), DatabaseTransactionError> {
        self.0
            .commit()
            .await
            .map_err(DatabaseTransactionError::SqlxError)
    }

    #[allow(dead_code)]
    pub async fn rollback(self) -> Result<(), DatabaseTransactionError> {
        self.0
            .rollback()
            .await
            .map_err(DatabaseTransactionError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
