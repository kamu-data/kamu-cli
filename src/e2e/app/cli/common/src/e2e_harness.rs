// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::{KamuCliPuppet, NewWorkspaceOptions};
use sqlx::{MySqlPool, PgPool};

use crate::{KamuApiServerClient, api_server_e2e_test};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MULTITENANT_KAMU_CONFIG_WITH_DEFAULT_USER: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      auth:
        users:
          predefined:
            - accountName: kamu
              password: kamu.dev
              email: kamu@example.com
              properties: [ admin ]
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MULTITENANT_KAMU_CONFIG_WITH_RESTRICTED_ANONYMOUS: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      auth:
        allowAnonymous: false
        users:
          predefined:
            - accountName: kamu
              password: kamu.dev
              email: kamu@example.com
              properties: [ admin ]
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, PartialEq)]
enum PotentialWorkspace {
    NoWorkspace,
    #[default]
    SingleTenant,
    MultiTenant,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct KamuCliApiServerHarnessOptions {
    potential_workspace: PotentialWorkspace,
    env_vars: Vec<(String, String)>,
    frozen_system_time: Option<DateTime<Utc>>,
    kamu_config: Option<String>,
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

    pub fn with_custom_frozen_system_time(mut self, value: DateTime<Utc>) -> Self {
        self.frozen_system_time = Some(value);

        self
    }

    pub fn with_frozen_system_time(self) -> Self {
        let t = Utc.with_ymd_and_hms(2050, 1, 2, 3, 4, 5).unwrap();

        self.with_custom_frozen_system_time(t)
    }

    pub fn with_today_as_frozen_system_time(self) -> Self {
        let today = {
            let now = Utc::now();

            now.with_time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap())
                .unwrap()
        };

        self.with_custom_frozen_system_time(today)
    }

    pub fn with_kamu_config(mut self, content: &str) -> Self {
        self.kamu_config = Some(content.into());

        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct KamuCliApiServerHarness {
    options: KamuCliApiServerHarnessOptions,
}

impl KamuCliApiServerHarness {
    pub fn postgres(pg_pool: &PgPool, options: KamuCliApiServerHarnessOptions) -> Self {
        let db = pg_pool.connect_options();
        let kamu_config = indoc::formatdoc!(
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
            "#,
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        );

        Self::new(options, Some(kamu_config))
    }

    pub fn mysql(mysql_pool: &MySqlPool, options: KamuCliApiServerHarnessOptions) -> Self {
        let db = mysql_pool.connect_options();
        let kamu_config = indoc::formatdoc!(
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
                "#,
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        );

        Self::new(options, Some(kamu_config))
    }

    pub fn sqlite(options: KamuCliApiServerHarnessOptions) -> Self {
        // We don't need to specify sqlite database in config,
        // because kamu-cli will create the database on its own
        Self::new(options, None)
    }

    fn new(
        mut options: KamuCliApiServerHarnessOptions,
        generated_kamu_config: Option<String>,
    ) -> Self {
        let target_config =
            generated_kamu_config.map(|target| serde_yaml::from_str(&target).unwrap());
        let source_config = options
            .kamu_config
            .map(|source| serde_yaml::from_str(&source).unwrap());

        options.kamu_config = merge_yaml(target_config, source_config)
            .map(|yaml| serde_yaml::to_string(&yaml).unwrap());

        Self { options }
    }

    pub async fn run_api_server<Fixture, FixtureResult>(self, fixture: Fixture)
    where
        Fixture: FnOnce(KamuApiServerClient) -> FixtureResult,
        FixtureResult: Future<Output = ()> + Send + 'static,
    {
        let kamu = self.into_kamu().await;

        let workspace_path = kamu.workspace_path().to_path_buf();
        let e2e_data_file_path = kamu.get_e2e_output_data_path();
        let server_run_fut = kamu.start_api_server(e2e_data_file_path.clone());

        api_server_e2e_test(e2e_data_file_path, workspace_path, server_run_fut, fixture).await;
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
            frozen_system_time,
            kamu_config,
        } = self.options;

        let mut kamu = match potential_workspace {
            PotentialWorkspace::NoWorkspace => KamuCliPuppet::new("."),
            ws => {
                let is_multi_tenant = ws == PotentialWorkspace::MultiTenant;

                KamuCliPuppet::new_workspace_tmp_with(NewWorkspaceOptions {
                    is_multi_tenant,
                    kamu_config,
                    env_vars,
                    account: None,
                })
                .await
            }
        };

        kamu.set_system_time(frozen_system_time);

        kamu
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn merge_yaml(
    target: Option<serde_yaml::Value>,
    source: Option<serde_yaml::Value>,
) -> Option<serde_yaml::Value> {
    match (target, source) {
        (Some(mut target), Some(source)) => {
            let target_mapping = target.as_mapping_mut().unwrap();
            let serde_yaml::Value::Mapping(source_mapping) = source else {
                panic!("source is not a mapping: {source:?}")
            };

            target_mapping.extend(source_mapping);

            Some(target)
        }
        (target, None) => target,
        (None, generated_kamu_config) => generated_kamu_config,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
