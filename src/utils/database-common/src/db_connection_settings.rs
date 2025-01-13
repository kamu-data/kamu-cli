// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use crate::DatabaseProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatabaseConnectionSettings {
    pub provider: DatabaseProvider,
    pub database_name: String,
    pub host: String,
    pub port: Option<u16>,
    pub max_connections: Option<u32>,
    pub max_lifetime_secs: Option<u64>,
    pub acquire_timeout_secs: Option<u64>,
}

impl DatabaseConnectionSettings {
    pub fn new(
        provider: DatabaseProvider,
        database_name: String,
        host: String,
        port: Option<u16>,
        max_connections: Option<u32>,
        max_lifetime_secs: Option<u64>,
        acquire_timeout_secs: Option<u64>,
    ) -> Self {
        Self {
            provider,
            database_name,
            host,
            port,
            max_connections,
            max_lifetime_secs,
            acquire_timeout_secs,
        }
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or_else(|| self.provider.default_port())
    }

    pub fn sqlite_from(path: &Path) -> Self {
        Self {
            provider: DatabaseProvider::Sqlite,
            database_name: String::new(),
            host: String::from(path.to_str().unwrap()),
            port: None,
            max_connections: None,
            max_lifetime_secs: None,
            acquire_timeout_secs: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
