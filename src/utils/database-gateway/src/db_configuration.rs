// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use secrecy::{ExposeSecret, Secret};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatabaseConfiguration {
    pub provider: String,
    pub user: String,
    pub password: Secret<String>,
    pub database_name: String,
    pub host: String,
    pub port: u32,
}

impl DatabaseConfiguration {
    pub fn connection_string(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}",
            self.provider,
            self.user,
            self.password.expose_secret(),
            self.host,
            self.port,
            self.database_name
        )
    }

    pub fn local_postgres() -> Self {
        Self {
            provider: String::from("postgres"),
            user: String::from("root"),
            password: Secret::new(String::from("root")),
            database_name: String::from("kamu-api-server"),
            host: String::from("localhost"),
            port: 5432,
        }
    }

    pub fn local_mariadb() -> Self {
        Self {
            provider: String::from("mariadb"),
            user: String::from("root"),
            password: Secret::new(String::from("root")),
            database_name: String::from("kamu-api-server"),
            host: String::from("localhost"),
            port: 3306,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
