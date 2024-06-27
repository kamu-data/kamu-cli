// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::InternalError;
use secrecy::{ExposeSecret, Secret};
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresPlugin {}

#[component(pub)]
impl PostgresPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        connection_string: &Secret<String>,
    ) -> Result<(), DatabaseError> {
        let pg_pool = Self::open_pg_pool(connection_string)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        Ok(())
    }

    fn open_pg_pool(connection_string: &Secret<String>) -> Result<PgPool, DatabaseError> {
        PgPool::connect_lazy(connection_string.expose_secret()).map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordRefresher for PostgresPlugin {
    async fn refresh_password(&self, catalog: &Catalog) -> Result<(), InternalError> {
        let password_provider = catalog.get_one::<dyn DatabasePasswordProvider>().unwrap();
        let fresh_password = password_provider.provide_password().await?;
        if let Some(fresh_password) = fresh_password {
            let pg_pool = catalog.get_one::<PgPool>().unwrap();
            let db_credentials = catalog.get_one::<DatabaseCredentials>().unwrap();

            pg_pool.set_connect_options(
                PgConnectOptions::new()
                    .host(&db_credentials.host)
                    .username(&db_credentials.user)
                    .database(&db_credentials.database_name)
                    .password(fresh_password.expose_secret()),
            );
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
