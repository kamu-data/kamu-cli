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
use sqlx::mysql::MySqlConnectOptions;
use sqlx::MySqlPool;

use crate::{DatabaseCredentials, DatabasePasswordProvider, DatabasePasswordRefresher};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPasswordRefresher {
    password_provider: Arc<dyn DatabasePasswordProvider>,
    mysql_pool: Arc<MySqlPool>,
    db_credentials: Arc<DatabaseCredentials>,
}

#[component(pub)]
#[interface(dyn DatabasePasswordRefresher)]
impl MySqlPasswordRefresher {
    pub fn new(
        password_provider: Arc<dyn DatabasePasswordProvider>,
        mysql_pool: Arc<MySqlPool>,
        db_credentials: Arc<DatabaseCredentials>,
    ) -> Self {
        Self {
            password_provider,
            mysql_pool,
            db_credentials,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordRefresher for MySqlPasswordRefresher {
    #[tracing::instrument(level = "info", skip_all)]
    async fn refresh_password(&self) -> Result<(), InternalError> {
        let fresh_password = self.password_provider.provide_password().await?;
        if let Some(fresh_password) = fresh_password {
            self.mysql_pool.set_connect_options(
                MySqlConnectOptions::new()
                    .host(&self.db_credentials.host)
                    .username(&self.db_credentials.user)
                    .database(&self.db_credentials.database_name)
                    .password(fresh_password.expose_secret()),
            );
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
