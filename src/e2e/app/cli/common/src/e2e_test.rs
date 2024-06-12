// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};

use internal_error::InternalError;
use kamu_cli_wrapper::Kamu;
use regex::Regex;
use reqwest::Url;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////

pub async fn e2e_test<ServerRunFut, Fixture, FixtureFut>(
    server_addr: SocketAddrV4,
    server_run_fut: ServerRunFut,
    fixture: Fixture,
) where
    ServerRunFut: Future<Output = Result<(), InternalError>>,
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()>,
{
    let test_fut = async move {
        let base_url = Url::parse(&format!("http://{server_addr}")).unwrap();
        let kamu_api_server_client = KamuApiServerClient::new(base_url);

        kamu_api_server_client.ready().await?;
        {
            fixture(kamu_api_server_client.clone()).await;
        }
        kamu_api_server_client.shutdown().await?;

        Ok::<_, InternalError>(())
    };

    assert_matches!(tokio::try_join!(server_run_fut, test_fut), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////
// Database-related E2E bootstrapping functions
////////////////////////////////////////////////////////////////////////////////

pub async fn postgres_kamu_cli_e2e_test<F, Fut>(pg_pool: PgPool, fixture: F)
where
    F: FnOnce(KamuApiServerClient) -> Fut,
    Fut: Future<Output = ()>,
{
    let db = pg_pool.connect_options();
    let kamu = Kamu::new_workspace_tmp_with_kamu_config(
        format!(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                    database:
                        provider: postgres
                        host: {host}
                        user: {user}
                        password: {password}
                        databaseName: {database}
                "#
            ),
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        )
        .as_str(),
    )
    .await;

    // TODO: Random port support -- this unlocks parallel running
    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000);
    let server_run_fut = kamu.start_api_server(server_addr);

    e2e_test(server_addr, server_run_fut, fixture).await;
}

////////////////////////////////////////////////////////////////////////////////

pub async fn mysql_kamu_cli_e2e_test<F, Fut>(mysql_pool: MySqlPool, fixture: F)
where
    F: FnOnce(KamuApiServerClient) -> Fut,
    Fut: Future<Output = ()>,
{
    let db = mysql_pool.connect_options();
    let kamu = Kamu::new_workspace_tmp_with_kamu_config(
        format!(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                    database:
                        provider: mySql
                        host: {host}
                        user: {user}
                        password: {password}
                        databaseName: {database}
                "#
            ),
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        )
        .as_str(),
    )
    .await;

    // TODO: Random port support -- this unlocks parallel running
    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000);
    let server_run_fut = kamu.start_api_server(server_addr);

    e2e_test(server_addr, server_run_fut, fixture).await;
}

////////////////////////////////////////////////////////////////////////////////

pub async fn sqlite_kamu_cli_e2e_test<F, Fut>(sqlite_pool: SqlitePool, fixture: F)
where
    F: FnOnce(KamuApiServerClient) -> Fut,
    Fut: Future<Output = ()>,
{
    // Ugly way to get the path as the settings have a not-so-good signature:
    // SqliteConnectOptions::get_filename(self) -> Cow<'static, Path>
    //                                    ^^^^
    // Arc<T> + consuming = bad combo
    let database_path = {
        let re = Regex::new(r#"filename: "(.*)""#).unwrap();
        let connect_options = format!("{:#?}", sqlite_pool.connect_options());
        let re_groups = re.captures(connect_options.as_str()).unwrap();

        re_groups[1].to_string()
    };
    let kamu = Kamu::new_workspace_tmp_with_kamu_config(
        format!(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                    database:
                        provider: sqlite
                        databasePath: {path}
                "#
            ),
            path = database_path
        )
        .as_str(),
    )
    .await;

    // TODO: Random port support -- this unlocks parallel running
    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000);
    let server_run_fut = kamu.start_api_server(server_addr);

    e2e_test(server_addr, server_run_fut, fixture).await;
}

////////////////////////////////////////////////////////////////////////////////
// Macros
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! kamu_cli_e2e_test {
    (postgres_kamu_cli_e2e_test, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, postgres)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
            async fn [<$test_name>] (pg_pool: sqlx::PgPool) {
                postgres_kamu_cli_e2e_test (
                    pg_pool,
                    $test_package::$test_name,
                )
                .await;
            }
        }
    };
    (mysql_kamu_cli_e2e_test, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, mysql)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/mysql"))]
            async fn [<$test_name>] (mysql_pool: sqlx::MySqlPool) {
                mysql_kamu_cli_e2e_test (
                    mysql_pool,
                    $test_package::$test_name,
                )
                .await;
            }
        }
    };
    (sqlite_kamu_cli_e2e_test, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, sqlite)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/sqlite"))]
            async fn [<$test_name>] (sqlite_pool: sqlx::SqlitePool) {
                sqlite_kamu_cli_e2e_test (
                    sqlite_pool,
                    $test_package::$test_name,
                )
                .await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
