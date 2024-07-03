// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use secrecy::ExposeSecret;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::MySqlPool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPlugin {}

#[component(pub)]
impl MySqlPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(catalog_builder: &mut CatalogBuilder) {
        catalog_builder.add::<Self>();
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlPasswordRefresher>();
    }

    pub fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        db_connection_settings: &DatabaseConnectionSettings,
        db_credentials: Option<&DatabaseCredentials>,
    ) -> Result<Catalog, DatabaseError> {
        let mysql_pool = Self::open_mysql_pool(db_connection_settings, db_credentials);

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(mysql_pool)
            .build())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_mysql_pool(
        db_connection_settings: &DatabaseConnectionSettings,
        db_credentials: Option<&DatabaseCredentials>,
    ) -> MySqlPool {
        let mut mysql_options = MySqlConnectOptions::new()
            .host(&db_connection_settings.host)
            .port(db_connection_settings.port())
            .database(&db_connection_settings.database_name);

        if let Some(db_credentials) = db_credentials {
            mysql_options = mysql_options
                .username(db_credentials.user_name.expose_secret())
                .password(db_credentials.password.expose_secret());
        }

        MySqlPool::connect_lazy_with(mysql_options)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
