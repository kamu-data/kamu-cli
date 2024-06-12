// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};

use kamu_cli_wrapper::{Kamu, NewWorkspaceOptions};
use regex::Regex;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::{e2e_test, KamuApiServerClient};

////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct KamuCliApiServerHarnessOptions {
    pub is_multi_tenant: bool,
    pub env_vars: Option<Vec<(String, String)>>,
}

pub struct KamuCliApiServerHarness<Fixture, FixtureFut>
where
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()>,
{
    kamu_config: String,
    fixture: Fixture,
    options: Option<KamuCliApiServerHarnessOptions>,
}

impl<Fixture, FixtureFut> KamuCliApiServerHarness<Fixture, FixtureFut>
where
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()>,
{
    pub fn postgres(
        pg_pool: PgPool,
        fixture: Fixture,
        options: Option<KamuCliApiServerHarnessOptions>,
    ) -> Self {
        let db = pg_pool.connect_options();
        let kamu_config = format!(
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
        );

        Self::new(kamu_config, fixture, options)
    }

    pub fn mysql(
        mysql_pool: MySqlPool,
        fixture: Fixture,
        options: Option<KamuCliApiServerHarnessOptions>,
    ) -> Self {
        let db = mysql_pool.connect_options();
        let kamu_config = format!(
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
        );

        Self::new(kamu_config, fixture, options)
    }

    pub fn sqlite(
        sqlite_pool: SqlitePool,
        fixture: Fixture,
        options: Option<KamuCliApiServerHarnessOptions>,
    ) -> Self {
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

        let kamu_config = format!(
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
        );

        Self::new(kamu_config, fixture, options)
    }

    fn new(
        kamu_config: String,
        fixture: Fixture,
        options: Option<KamuCliApiServerHarnessOptions>,
    ) -> Self {
        Self {
            kamu_config,
            fixture,
            options,
        }
    }

    pub async fn run(self) {
        let KamuCliApiServerHarnessOptions {
            is_multi_tenant,
            env_vars,
        } = self.options.unwrap_or_default();
        let kamu = Kamu::new_workspace_tmp_with(NewWorkspaceOptions {
            is_multi_tenant,
            kamu_config: Some(self.kamu_config),
            env_vars,
        })
        .await;

        // TODO: Random port support -- this unlocks parallel running
        let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4000);
        let server_run_fut = kamu.start_api_server(server_addr);

        e2e_test(server_addr, server_run_fut, self.fixture).await;
    }
}

////////////////////////////////////////////////////////////////////////////////
