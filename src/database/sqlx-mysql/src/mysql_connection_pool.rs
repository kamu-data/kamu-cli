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
use sqlx::{MySql, MySqlPool, Transaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySQLConnectionPool {
    mysql_pool: MySqlPool,
}

impl MySQLConnectionPool {
    pub fn new(db_configuration: &DatabaseConfiguration) -> Result<Self, DatabaseError> {
        Ok(Self {
            mysql_pool: MySqlPool::connect_lazy(db_configuration.connection_string().as_str())
                .map_err(DatabaseError::SqlxError)?,
        })
    }

    pub async fn begin_transaction<'c>(
        &self,
    ) -> Result<MySqlConnectionPoolTransaction<'c>, DatabaseError> {
        let mysql_transaction = self
            .mysql_pool
            .begin()
            .await
            .map_err(DatabaseError::SqlxError)?;

        Ok(MySqlConnectionPoolTransaction(mysql_transaction))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlConnectionPoolTransaction<'c>(Transaction<'c, MySql>);

impl<'c> std::ops::Deref for MySqlConnectionPoolTransaction<'c> {
    type Target = Transaction<'c, MySql>;

    fn deref(&self) -> &Self::Target {
        self.0.borrow()
    }
}

impl<'c> std::ops::DerefMut for MySqlConnectionPoolTransaction<'c> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.borrow_mut()
    }
}

impl<'c> MySqlConnectionPoolTransaction<'c> {
    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.0.commit().await.map_err(DatabaseError::SqlxError)
    }

    #[allow(dead_code)]
    pub async fn rollback(self) -> Result<(), DatabaseError> {
        self.0.rollback().await.map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
