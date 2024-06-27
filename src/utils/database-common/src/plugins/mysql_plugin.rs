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
use sqlx::mysql::MySqlConnectOptions;
use sqlx::MySqlPool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPlugin {}

#[component(pub)]
#[interface(dyn DatabasePasswordRefresher)]
impl MySqlPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        connection_string: &Secret<String>,
    ) -> Result<(), DatabaseError> {
        let mysql_pool = Self::open_mysql_pool(connection_string)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();

        Ok(())
    }

    fn open_mysql_pool(connection_string: &Secret<String>) -> Result<MySqlPool, DatabaseError> {
        MySqlPool::connect_lazy(connection_string.expose_secret()).map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordRefresher for MySqlPlugin {
    async fn refresh_password(&self, catalog: &Catalog) -> Result<(), InternalError> {
        let password_provider = catalog.get_one::<dyn DatabasePasswordProvider>().unwrap();
        let fresh_password = password_provider.provide_password().await?;
        if let Some(fresh_password) = fresh_password {
            let my_sql_pool = catalog.get_one::<MySqlPool>().unwrap();
            let db_credentials = catalog.get_one::<DatabaseCredentials>().unwrap();

            my_sql_pool.set_connect_options(
                MySqlConnectOptions::new()
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
