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
use reqwest::Url;
use sqlx::PgPool;

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

#[macro_export]
macro_rules! kamu_cli_e2e_test {
    (postgres_kamu_cli_e2e_test, $test_package: expr, $test_name: expr) => {
        paste::paste! {
            #[test_group::group(e2e, database, postgres)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
            async fn [<$test_name>] (pool: sqlx::PgPool) {
                postgres_kamu_cli_e2e_test (
                    pool,
                    $test_package::$test_name,
                )
                .await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
