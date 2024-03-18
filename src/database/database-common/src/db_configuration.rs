// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use secrecy::{ExposeSecret, Secret};

use crate::DatabaseProvider;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatabaseConfiguration {
    pub provider: DatabaseProvider,
    pub user: String,
    pub password: Secret<String>,
    pub database_name: String,
    pub host: String,
    pub port: Option<u32>,
}

impl DatabaseConfiguration {
    pub fn connection_string(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}",
            self.provider,
            self.user,
            self.password.expose_secret(),
            self.host,
            self.port.unwrap_or_else(|| self.provider.default_port()),
            self.database_name
        )
    }

    pub fn local_postgres() -> Self {
        Self {
            provider: DatabaseProvider::Postgres,
            user: String::from("root"),
            password: Secret::new(String::from("root")),
            database_name: String::from("kamu-api-server"),
            host: String::from("localhost"),
            port: None,
        }
    }

    pub fn local_mariadb() -> Self {
        Self {
            provider: DatabaseProvider::MariaDB,
            user: String::from("root"),
            password: Secret::new(String::from("root")),
            database_name: String::from("kamu-api-server"),
            host: String::from("localhost"),
            port: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
