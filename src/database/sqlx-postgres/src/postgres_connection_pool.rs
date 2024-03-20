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
use sqlx::{PgPool, Postgres, Transaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresConnectionPool {
    pg_pool: PgPool,
}

#[component(pub)]
#[scope(Singleton)]
impl PostgresConnectionPool {
    pub fn new(pg_pool: PgPool) -> Self {
        Self { pg_pool }
    }

    pub async fn begin_transaction(&self) -> Result<PostgresTransaction, DatabaseError> {
        self.pg_pool.begin().await.map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type PostgresTransaction = Transaction<'static, Postgres>;

/////////////////////////////////////////////////////////////////////////////////////////
