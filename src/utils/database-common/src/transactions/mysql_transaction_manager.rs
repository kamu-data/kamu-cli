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
use sqlx::MySqlPool;

use crate::{DatabaseTransactionManager, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlTransactionManager {
    mysql_pool: MySqlPool,
}

#[component(pub)]
#[interface(dyn DatabaseTransactionManager)]
impl MySqlTransactionManager {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        Self { mysql_pool }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for MySqlTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        Ok(TransactionRef::new(self.mysql_pool.clone()))
    }

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::MySql>();
        let maybe_open_mysql_transaction = transaction_typed.into_maybe_transaction();
        if let Some(mysql_transaction) = maybe_open_mysql_transaction {
            mysql_transaction.commit().await.int_err()?;
        }

        Ok(())
    }

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_typed = transaction_ref.downcast::<sqlx::MySql>();
        let maybe_open_mysql_transaction = transaction_typed.into_maybe_transaction();
        if let Some(mysql_transaction) = maybe_open_mysql_transaction {
            mysql_transaction.rollback().await.int_err()?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
