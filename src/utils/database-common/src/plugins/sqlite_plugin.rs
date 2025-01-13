// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("../../../migrations/sqlite");

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

    pub async fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        db_connection_settings: &DatabaseConnectionSettings,
    ) -> Result<Catalog, DatabaseError> {
        let sqlite_pool = Self::open_sqlite_pool(db_connection_settings);

        SQLITE_MIGRATOR
            .run(&sqlite_pool)
            .await
            .expect("Migration failed");

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(sqlite_pool)
            .build())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_sqlite_pool(db_connection_settings: &DatabaseConnectionSettings) -> SqlitePool {
        let sqlite_options = SqliteConnectOptions::new()
            .filename(&db_connection_settings.host)
            .create_if_missing(true);

        if db_connection_settings.max_lifetime_secs.is_some() {
            tracing::warn!("max_lifetime_secs is not supported for SQLite");
        }
        if db_connection_settings.max_connections.is_some() {
            tracing::warn!("max_connections is not supported for SQLite");
        }
        if db_connection_settings.acquire_timeout_secs.is_some() {
            tracing::warn!("acquire_timeout_secs is not supported for SQLite");
        }

        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_lazy_with(sqlite_options)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
