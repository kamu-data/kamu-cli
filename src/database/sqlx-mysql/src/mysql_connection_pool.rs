// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::DatabaseError;
use dill::{component, scope, Singleton};
use sqlx::{MySql, MySqlPool, Transaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlConnectionPool {
    mysql_pool: MySqlPool,
}

#[component(pub)]
#[scope(Singleton)]
impl MySqlConnectionPool {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        Self { mysql_pool }
    }

    pub(crate) async fn begin_transaction(&self) -> Result<MySqlTransaction, DatabaseError> {
        self.mysql_pool
            .begin()
            .await
            .map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type MySqlTransaction = Transaction<'static, MySql>;

/////////////////////////////////////////////////////////////////////////////////////////
