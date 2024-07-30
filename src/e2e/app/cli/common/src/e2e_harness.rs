// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use chrono::{DateTime, NaiveTime, Utc};
use kamu_cli_puppet::{KamuCliPuppet, NewWorkspaceOptions};
use kamu_cli_puppet_ext::KamuCliPuppetExt;
use regex::Regex;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::{api_server_e2e_test, KamuApiServerClient};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, PartialEq)]
enum PotentialWorkspace {
    NoWorkspace,
    #[default]
    SingleTenant,
    MultiTenant,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct KamuCliApiServerHarnessOptions {
    potential_workspace: PotentialWorkspace,
    env_vars: Vec<(String, String)>,
    frozen_system_time: Option<DateTime<Utc>>,
}

impl KamuCliApiServerHarnessOptions {
    pub fn with_no_workspace(mut self) -> Self {
        self.potential_workspace = PotentialWorkspace::NoWorkspace;

        self
    }

    pub fn with_multi_tenant(mut self) -> Self {
        self.potential_workspace = PotentialWorkspace::MultiTenant;

        self.env_vars.extend([
            ("KAMU_AUTH_GITHUB_CLIENT_ID".into(), "1".into()),
            ("KAMU_AUTH_GITHUB_CLIENT_SECRET".into(), "2".into()),
        ]);

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
    options: KamuCliApiServerHarnessOptions,
}

impl KamuCliApiServerHarness {
    pub fn inmem(options: KamuCliApiServerHarnessOptions) -> Self {
        Self::new(options, None)
    }

    pub fn postgres(pg_pool: &PgPool, options: KamuCliApiServerHarnessOptions) -> Self {
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
                        credentialsPolicy:
                            source:
                                kind: rawPassword
                                userName: {user}
                                rawPassword: {password}
                        databaseName: {database}
                "#
            ),
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        );

        Self::new(options, Some(kamu_config))
    }

    pub fn mysql(mysql_pool: &MySqlPool, options: KamuCliApiServerHarnessOptions) -> Self {
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
                        credentialsPolicy:
                            source:
                                kind: rawPassword
                                userName: {user}
                                rawPassword: {password}
                        databaseName: {database}
                "#
            ),
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        );

        Self::new(options, Some(kamu_config))
    }

    pub fn sqlite(sqlite_pool: &SqlitePool, options: KamuCliApiServerHarnessOptions) -> Self {
        // Ugly way to get the path as the settings have a not-so-good signature:
        // SqliteConnectOptions::get_filename(self) -> Cow<'static, Path>
        //                                    ^^^^
        // Arc<T> + consuming = bad combo
        let database_path = {
            let re = Regex::new(r#"filename: "(.*)""#).unwrap();
            let connect_options = format!("{:#?}", sqlite_pool.connect_options());
            let re_groups = re.captures(connect_options.as_str()).unwrap();
            let relative_path = re_groups[1].to_string();

            std::fs::canonicalize(relative_path)
                .unwrap()
                .display()
                .to_string()
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

        Self::new(options, Some(kamu_config))
    }

    fn new(options: KamuCliApiServerHarnessOptions, kamu_config: Option<String>) -> Self {
        Self {
            options,
            kamu_config,
        }
    }

    pub async fn run_api_server<Fixture, FixtureResult>(self, fixture: Fixture)
    where
        Fixture: FnOnce(KamuApiServerClient) -> FixtureResult,
        FixtureResult: Future<Output = ()> + Send + 'static,
    {
        let kamu = self.into_kamu().await;

        let e2e_data_file_path = kamu.get_e2e_output_data_path();
        let server_run_fut = kamu.start_api_server(e2e_data_file_path.clone());

        api_server_e2e_test(e2e_data_file_path, server_run_fut, fixture).await;
    }

    pub async fn execute_command<Fixture, FixtureResult>(self, fixture: Fixture)
    where
        Fixture: FnOnce(KamuCliPuppet) -> FixtureResult,
        FixtureResult: Future<Output = ()>,
    {
        let kamu = self.into_kamu().await;

        fixture(kamu).await;
    }

    async fn into_kamu(self) -> KamuCliPuppet {
        let KamuCliApiServerHarnessOptions {
            potential_workspace,
            env_vars,
            frozen_system_time: freeze_system_time,
        } = self.options;

        let mut kamu = match potential_workspace {
            PotentialWorkspace::NoWorkspace => KamuCliPuppet::new("."),
            ws => {
                let is_multi_tenant = ws == PotentialWorkspace::MultiTenant;

                KamuCliPuppet::new_workspace_tmp_with(NewWorkspaceOptions {
                    is_multi_tenant,
                    kamu_config: self.kamu_config,
                    env_vars,
                })
                .await
            }
        };

        kamu.set_system_time(freeze_system_time);

        kamu
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
