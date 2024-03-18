// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::{Borrow, BorrowMut};

use database_common::{DatabaseConfiguration, DatabaseError};
use dill::{component, scope, Singleton};
use sqlx::{PgPool, Postgres, Transaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresConnectionPool {
    pg_pool: PgPool,
}

#[component(pub)]
#[scope(Singleton)]
impl PostgresConnectionPool {
    pub fn build_pg_pool(
        db_configuration: &DatabaseConfiguration,
    ) -> Result<PgPool, DatabaseError> {
        PgPool::connect_lazy(db_configuration.connection_string().as_str())
            .map_err(DatabaseError::SqlxError)
    }

    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }

    pub async fn begin_transaction<'c>(
        &self,
    ) -> Result<PostgresConnectionPoolTransaction<'c>, DatabaseError> {
        let pg_transaction = self
            .pg_pool
            .begin()
            .await
            .map_err(DatabaseError::SqlxError)?;

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
    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.0.commit().await.map_err(DatabaseError::SqlxError)
    }

    #[allow(dead_code)]
    pub async fn rollback(self) -> Result<(), DatabaseError> {
        self.0.rollback().await.map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
