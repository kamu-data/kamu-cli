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

use crate::{MySqlConnectionPool, MySqlTransaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlTransactionManager {
    mysql_connection_pool: Arc<MySqlConnectionPool>,
}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl MySqlTransactionManager {
    pub fn new(mysql_connection_pool: Arc<MySqlConnectionPool>) -> Self {
        Self {
            mysql_connection_pool,
        }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for MySqlTransactionManager {
    async fn make_transaction(&self) -> Result<Transaction, InternalError> {
        let mysql_transaction = self
            .mysql_connection_pool
            .begin_transaction()
            .await
            .int_err()?;
        Ok(Transaction::new(mysql_transaction))
    }

    async fn commit_transaction(&self, transaction: Transaction) -> Result<(), InternalError> {
        let mysql_transaction = transaction
            .transaction
            .downcast::<MySqlTransaction>()
            .unwrap();

        mysql_transaction.commit().await.int_err()?;
        Ok(())
    }

    async fn rollback_transaction(&self, transaction: Transaction) -> Result<(), InternalError> {
        let mysql_transaction = transaction
            .transaction
            .downcast::<MySqlTransaction>()
            .unwrap();

        mysql_transaction.rollback().await.int_err()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
