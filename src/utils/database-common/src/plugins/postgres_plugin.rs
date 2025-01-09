// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use dill::*;
use secrecy::ExposeSecret;
use sqlx::pool::PoolOptions;
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Postgres};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresPlugin {}

#[component(pub)]
impl PostgresPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(catalog_builder: &mut CatalogBuilder) {
        catalog_builder.add::<Self>();
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresPasswordRefresher>();
    }

    pub fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        db_connection_settings: &DatabaseConnectionSettings,
        db_credentials: Option<&DatabaseCredentials>,
    ) -> Result<Catalog, DatabaseError> {
        let pg_pool = Self::open_pg_pool(db_connection_settings, db_credentials);

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(pg_pool)
            .build())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_pg_pool(
        db_connection_settings: &DatabaseConnectionSettings,
        db_credentials: Option<&DatabaseCredentials>,
    ) -> PgPool {
        let mut pg_options = PgConnectOptions::new()
            .host(&db_connection_settings.host)
            .port(db_connection_settings.port())
            .database(&db_connection_settings.database_name);

        if let Some(db_credentials) = db_credentials {
            pg_options = pg_options
                .username(db_credentials.user_name.expose_secret())
                .password(db_credentials.password.expose_secret());
        }

        let pg_pool: PoolOptions<Postgres> = PoolOptions::new()
            .max_lifetime(
                db_connection_settings
                    .max_lifetime_secs
                    .map(Duration::from_secs),
            )
            .max_connections(db_connection_settings.max_connections.unwrap_or(10))
            .acquire_timeout(
                db_connection_settings
                    .acquire_timeout_secs
                    .map_or(Duration::from_secs(30), Duration::from_secs),
            );

        pg_pool.connect_lazy_with(pg_options)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
