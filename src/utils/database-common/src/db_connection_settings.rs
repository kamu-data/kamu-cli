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
}

impl DatabaseConnectionSettings {
    pub fn new(
        provider: DatabaseProvider,
        database_name: String,
        host: String,
        port: Option<u16>,
    ) -> Self {
        Self {
            provider,
            database_name,
            host,
            port,
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
