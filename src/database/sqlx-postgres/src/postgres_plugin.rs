// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{
    DatabaseConfiguration,
    DatabaseError,
    DatabaseTransactionManager,
    Transaction,
};
use dill::*;
use kamu_core::{InternalError, ResultIntoInternal};
use sqlx::PgPool;

use crate::{PostgresAccountRepository, PostgresConnectionPool, PostgresTransaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresPlugin {}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl PostgresPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), DatabaseError> {
        let pg_pool = Self::open_pg_pool(db_configuration)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_builder(PostgresConnectionPool::builder().with_pg_pool(pg_pool));
        catalog_builder.add::<PostgresAccountRepository>();

        Ok(())
    }

    fn open_pg_pool(db_configuration: &DatabaseConfiguration) -> Result<PgPool, DatabaseError> {
        PgPool::connect_lazy(db_configuration.connection_string().as_str())
            .map_err(DatabaseError::SqlxError)
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for PostgresPlugin {
    async fn make_transaction(&self, base_catalog: &Catalog) -> Result<Transaction, InternalError> {
        let postgres_connection_pool = base_catalog.get_one::<PostgresConnectionPool>().unwrap();
        let postgres_transaction = postgres_connection_pool
            .begin_transaction()
            .await
            .int_err()?;
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
