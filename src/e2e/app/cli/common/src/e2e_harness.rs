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

use internal_error::InternalError;
use kamu_cli::testing::{Kamu, NewWorkspaceOptions};
use regex::Regex;
use sqlx::types::chrono::{DateTime, NaiveTime, Utc};
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::{api_server_e2e_test, KamuApiServerClient};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct KamuCliApiServerHarnessOptions {
    pub is_multi_tenant: bool,
    pub env_vars: Vec<(String, String)>,
    pub frozen_system_time: Option<DateTime<Utc>>,
}

impl KamuCliApiServerHarnessOptions {
    pub fn with_multi_tenant(mut self, enabled: bool) -> Self {
        self.is_multi_tenant = enabled;

        if enabled {
            self.env_vars.extend([
                ("KAMU_AUTH_GITHUB_CLIENT_ID".into(), "1".into()),
                ("KAMU_AUTH_GITHUB_CLIENT_SECRET".into(), "2".into()),
            ]);
        }

        self
    }

    pub fn with_frozen_system_time(mut self, value: DateTime<Utc>) -> Self {
        self.frozen_system_time = Some(value);

        self
    }

    pub fn with_today_as_frozen_system_time(self) -> Self {
        let today = {
            let now = Utc::now();

            now.with_time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap())
                .unwrap()
        };

        self.with_frozen_system_time(today)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct KamuCliApiServerHarness {
    kamu_config: Option<String>,
    options: Option<KamuCliApiServerHarnessOptions>,
}

impl KamuCliApiServerHarness {
    pub fn inmem(options: Option<KamuCliApiServerHarnessOptions>) -> Self {
        Self::new(None, options)
    }

    pub fn postgres(pg_pool: &PgPool, options: Option<KamuCliApiServerHarnessOptions>) -> Self {
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

        Self::new(Some(kamu_config), options)
    }

    pub fn mysql(mysql_pool: &MySqlPool, options: Option<KamuCliApiServerHarnessOptions>) -> Self {
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

        Self::new(Some(kamu_config), options)
    }

    pub fn sqlite(
        sqlite_pool: &SqlitePool,
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

        Self::new(Some(kamu_config), options)
    }

    fn new(kamu_config: Option<String>, options: Option<KamuCliApiServerHarnessOptions>) -> Self {
        Self {
            kamu_config,
            options,
        }
    }

    pub async fn run_api_server<Fixture, FixtureResult>(self, fixture: Fixture)
    where
        Fixture: FnOnce(KamuApiServerClient) -> FixtureResult,
        FixtureResult: Future<Output = ()>,
    {
        let kamu = self.into_kamu().await;

        let server_addr = kamu.get_server_address();
        let server_run_fut = kamu.start_api_server(server_addr);

        api_server_e2e_test(server_addr, server_run_fut, fixture).await;
    }

    pub async fn execute_command<Fixture, FixtureResult>(self, fixture: Fixture)
    where
        Fixture: FnOnce(Kamu) -> FixtureResult,
        FixtureResult: Future<Output = Result<(), InternalError>>,
    {
        let kamu = self.into_kamu().await;

        let execute_result = fixture(kamu).await;

        assert_matches!(execute_result, Ok(()));
    }

    async fn into_kamu(self) -> Kamu {
        let KamuCliApiServerHarnessOptions {
            is_multi_tenant,
            env_vars,
            frozen_system_time: freeze_system_time,
        } = self.options.unwrap_or_default();
        let mut kamu = Kamu::new_workspace_tmp_with(NewWorkspaceOptions {
            is_multi_tenant,
            kamu_config: self.kamu_config,
            env_vars,
        })
        .await;

        kamu.set_system_time(freeze_system_time);

        kamu
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
