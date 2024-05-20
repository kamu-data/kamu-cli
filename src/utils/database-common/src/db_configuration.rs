// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

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
    pub fn new(
        provider: DatabaseProvider,
        user: String,
        password: String,
        database_name: String,
        host: String,
        port: Option<u32>,
    ) -> Self {
        Self {
            provider,
            user,
            password: Secret::from(password),
            database_name,
            host,
            port,
        }
    }

    pub fn connection_string(&self) -> String {
        if let DatabaseProvider::Sqlite = self.provider {
            format!("{}://{}", self.provider, self.database_name)
        } else {
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
    }

    pub fn connection_string_no_db(&self) -> String {
        if let DatabaseProvider::Sqlite = self.provider {
            panic!("Sqlite does not support connection strings without DB")
        } else {
            format!(
                "{}://{}:{}@{}:{}",
                self.provider,
                self.user,
                self.password.expose_secret(),
                self.host,
                self.port.unwrap_or_else(|| self.provider.default_port()),
            )
        }
    }

    pub fn sqlite_from(path: &Path) -> Self {
        Self {
            provider: DatabaseProvider::Sqlite,
            user: String::new(),
            password: Secret::new(String::new()),
            database_name: String::from(path.to_str().unwrap()),
            host: String::new(),
            port: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
