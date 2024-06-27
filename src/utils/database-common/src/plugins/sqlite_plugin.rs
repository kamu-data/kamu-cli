// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use secrecy::Secret;
use sqlx::SqlitePool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlitePlugin {}

#[component(pub)]
impl SqlitePlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_credentials: &DatabaseCredentials,
        db_password: Option<&Secret<String>>,
    ) -> Result<(), DatabaseError> {
        let sqlite_pool = Self::open_sqlite_pool(db_credentials, db_password)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        Ok(())
    }

    fn open_sqlite_pool(
        db_credentials: &DatabaseCredentials,
        db_password: Option<&Secret<String>>,
    ) -> Result<SqlitePool, DatabaseError> {
        let connection_string = db_credentials.connection_string(db_password);
        SqlitePool::connect_lazy(&connection_string).map_err(DatabaseError::SqlxError)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
