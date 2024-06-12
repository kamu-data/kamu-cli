// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]

mod e2e_hartness;
mod e2e_test;
mod kamu_api_server_client;

pub use e2e_hartness::*;
pub use e2e_test::*;
pub use kamu_api_server_client::*;

pub mod prelude;

////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! kamu_cli_e2e_test {
    (postgres, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, postgres)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
            async fn [<$test_name>] (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(pg_pool, $test_package::$test_name, None).run().await;
            }
        }
    };
    (postgres, $test_package: expr, $test_name: expr, $test_options: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, postgres)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
            async fn [<$test_name>] (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(pg_pool, $test_package::$test_name, Some($test_options)).run().await;
            }
        }
    };
    (mysql, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, mysql)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/mysql"))]
            async fn [<$test_name>] (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(mysql_pool, $test_package::$test_name, None).run().await;
            }
        }
    };
    (mysql, $test_package: expr, $test_name: expr, $test_options: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, mysql)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/mysql"))]
            async fn [<$test_name>] (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(mysql_pool, $test_package::$test_name, Some($test_options)).run().await;
            }
        }
    };
    (sqlite, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, sqlite)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/sqlite"))]
            async fn [<$test_name>] (sqlite_pool: sqlx::SqlitePool) {
                KamuCliApiServerHarness::sqlite(sqlite_pool, $test_package::$test_name, None).run().await;
            }
        }
    };
    (sqlite, $test_package: expr, $test_name: expr, $test_options: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, sqlite)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/sqlite"))]
            async fn [<$test_name>] (sqlite_pool: sqlx::SqlitePool) {
                KamuCliApiServerHarness::sqlite(sqlite_pool, $test_package::$test_name, Some($test_options)).run().await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
