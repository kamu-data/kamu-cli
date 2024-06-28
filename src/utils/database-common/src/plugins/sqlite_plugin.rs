// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use secrecy::{ExposeSecret, Secret};
use sqlx::SqlitePool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlitePlugin {}

#[component(pub)]
impl SqlitePlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(catalog_builder: &mut CatalogBuilder) {
        catalog_builder.add::<Self>();
        catalog_builder.add::<SqliteTransactionManager>();
    }

    pub fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        connection_string: &Secret<String>,
    ) -> Result<Catalog, DatabaseError> {
        let sqlite_pool = Self::open_sqlite_pool(connection_string)?;

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(sqlite_pool)
            .build())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_sqlite_pool(connection_string: &Secret<String>) -> Result<SqlitePool, DatabaseError> {
        SqlitePool::connect_lazy(connection_string.expose_secret())
            .map_err(DatabaseError::SqlxError)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
