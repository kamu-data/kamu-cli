// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DatabaseProvider {
    Postgres,
    MySQL,
    MariaDB,
}

impl DatabaseProvider {
    pub fn default_port(&self) -> u32 {
        match self {
            DatabaseProvider::MariaDB | DatabaseProvider::MySQL => 3306,
            DatabaseProvider::Postgres => 5432,
        }
    }
}

impl std::fmt::Display for DatabaseProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DatabaseProvider::Postgres => "postgres",
                DatabaseProvider::MySQL => "mysql",
                DatabaseProvider::MariaDB => "mariadb",
            }
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
