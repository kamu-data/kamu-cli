// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use secrecy::ExposeSecret;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;

use crate::{DatabaseConnectionSettings, DatabasePasswordProvider, DatabasePasswordRefresher};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPasswordRefresher {
    password_provider: Arc<dyn DatabasePasswordProvider>,
    mysql_pool: Arc<MySqlPool>,
    db_connection_settings: Arc<DatabaseConnectionSettings>,
}

#[component(pub)]
#[interface(dyn DatabasePasswordRefresher)]
impl MySqlPasswordRefresher {
    pub fn new(
        password_provider: Arc<dyn DatabasePasswordProvider>,
        mysql_pool: Arc<MySqlPool>,
        db_connection_settings: Arc<DatabaseConnectionSettings>,
    ) -> Self {
        Self {
            password_provider,
            mysql_pool,
            db_connection_settings,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordRefresher for MySqlPasswordRefresher {
    #[tracing::instrument(level = "info", skip_all)]
    async fn refresh_password(&self) -> Result<(), InternalError> {
        let fresh_credentials = self.password_provider.provide_credentials().await?;
        if let Some(fresh_credentials) = fresh_credentials {
            self.mysql_pool.set_connect_options(
                MySqlConnectOptions::new()
                    .host(&self.db_connection_settings.host)
                    .username(fresh_credentials.user_name.expose_secret())
                    .database(&self.db_connection_settings.database_name)
                    .password(fresh_credentials.password.expose_secret()),
            );
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
