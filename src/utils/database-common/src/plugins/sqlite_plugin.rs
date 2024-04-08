// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use sqlx::SqlitePool;

use crate::{DatabaseConfiguration, DatabaseError, SqliteTransactionManager};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlitePlugin {}

#[component(pub)]
impl SqlitePlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), DatabaseError> {
        let sqlite_pool = Self::open_sqlite_pool(db_configuration)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        Ok(())
    }

    fn open_sqlite_pool(
        db_configuration: &DatabaseConfiguration,
    ) -> Result<SqlitePool, DatabaseError> {
        SqlitePool::connect_lazy(db_configuration.connection_string().as_str())
            .map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
