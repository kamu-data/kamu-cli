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
    TransactionSubject,
};
use dill::*;
use kamu_core::{InternalError, ResultIntoInternal};
use sqlx::MySqlPool;

use crate::{MySqlAccountRepository, MySqlConnectionPool, MySqlTransaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPlugin {}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl MySqlPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), InternalError> {
        let mysql_pool = Self::build_mysql_pool(db_configuration).int_err()?;

        catalog_builder.add::<Self>();
        catalog_builder.add_builder(MySqlConnectionPool::builder().with_mysql_pool(mysql_pool));
        catalog_builder.add::<MySqlAccountRepository>();

        Ok(())
    }

    fn build_mysql_pool(
        db_configuration: &DatabaseConfiguration,
    ) -> Result<MySqlPool, DatabaseError> {
        MySqlPool::connect_lazy(db_configuration.connection_string().as_str())
            .map_err(DatabaseError::SqlxError)
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for MySqlPlugin {
    async fn make_transaction_subject(
        &self,
        base_catalog: &Catalog,
    ) -> Result<TransactionSubject, InternalError> {
        let mysql_connection_pool = base_catalog.get_one::<MySqlConnectionPool>().unwrap();
        let mysql_transaction = mysql_connection_pool.begin_transaction().await.int_err()?;
        Ok(TransactionSubject::new(mysql_transaction))
    }

    async fn commit_transaction(
        &self,
        transaction_subject: TransactionSubject,
    ) -> Result<(), InternalError> {
        let mysql_transaction = transaction_subject
            .transaction
            .downcast::<MySqlTransaction>()
            .unwrap();

        mysql_transaction.commit().await.int_err()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
