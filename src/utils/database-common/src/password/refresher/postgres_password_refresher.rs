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
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;

use crate::{DatabaseConnectionSettings, DatabasePasswordProvider, DatabasePasswordRefresher};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresPasswordRefresher {
    password_provider: Arc<dyn DatabasePasswordProvider>,
    pg_pool: Arc<PgPool>,
    db_connection_settings: Arc<DatabaseConnectionSettings>,
}

#[component(pub)]
#[interface(dyn DatabasePasswordRefresher)]
impl PostgresPasswordRefresher {
    pub fn new(
        password_provider: Arc<dyn DatabasePasswordProvider>,
        pg_pool: Arc<PgPool>,
        db_connection_settings: Arc<DatabaseConnectionSettings>,
    ) -> Self {
        Self {
            password_provider,
            pg_pool,
            db_connection_settings,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordRefresher for PostgresPasswordRefresher {
    #[tracing::instrument(level = "info", skip_all)]
    async fn refresh_password(&self) -> Result<(), InternalError> {
        let fresh_credentials = self.password_provider.provide_credentials().await?;
        if let Some(fresh_credentials) = fresh_credentials {
            self.pg_pool.set_connect_options(
                PgConnectOptions::new()
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
