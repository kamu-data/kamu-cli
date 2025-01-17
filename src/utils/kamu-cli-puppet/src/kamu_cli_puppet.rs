// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::{ffi, fs};

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ExecuteCommandResult = assert_cmd::assert::Assert;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct KamuCliPuppet {
    workspace_path: PathBuf,
    system_time: Option<DateTime<Utc>>,
    account: Option<odf::AccountName>,
    temp_dir: Option<tempfile::TempDir>,
}

impl KamuCliPuppet {
    pub fn new<P: Into<PathBuf>>(workspace_path: P) -> Self {
        let workspace_path = workspace_path.into();

        Self {
            workspace_path,
            system_time: None,
            account: None,
            temp_dir: None,
        }
    }

    pub async fn new_workspace_tmp(is_multi_tenant: bool) -> Self {
        if is_multi_tenant {
            Self::new_workspace_tmp_multi_tenant().await
        } else {
            Self::new_workspace_tmp_single_tenant().await
        }
    }

    pub async fn new_workspace_tmp_single_tenant() -> Self {
        Self::new_workspace_tmp_with(NewWorkspaceOptions::default()).await
    }

    pub async fn new_workspace_tmp_multi_tenant() -> Self {
        Self::new_workspace_tmp_with(NewWorkspaceOptions {
            is_multi_tenant: true,
            kamu_config: Some(
                indoc::indoc!(
                    r#"
                    kind: CLIConfig
                    version: 1
                    content:
                      users:
                        predefined:
                          - accountName: e2e-user
                    "#
                )
                .into(),
            ),
            account: Some(odf::AccountName::new_unchecked("e2e-user")),
            ..Default::default()
        })
        .await
    }

    pub async fn new_workspace_tmp_with(options: NewWorkspaceOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        if let Some(config) = options.kamu_config {
            fs::write(temp_dir.path().join(".kamuconfig"), config).unwrap();
        }

        let inst = Self::new(temp_dir.path());
        let inst = Self {
            temp_dir: Some(temp_dir),
            ..inst
        };

        let arguments = if options.is_multi_tenant {
            vec!["init", "--multi-tenant"]
        } else {
            vec!["init"]
        };

        for (env_name, env_value) in options.env_vars {
            std::env::set_var(env_name, env_value);
        }

        inst.execute(arguments).await.success();

        inst
    }

    pub fn set_system_time(&mut self, t: Option<DateTime<Utc>>) {
        self.system_time = t;
    }

    pub fn set_account(&mut self, account: Option<odf::AccountName>) {
        self.account = account;
    }

    pub fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    pub fn set_workspace_path_in_tmp_dir(&mut self) {
        let temp_dir = tempfile::tempdir().unwrap();

        self.workspace_path = temp_dir.path().into();
        self.temp_dir = Some(temp_dir);
    }

    pub fn get_e2e_output_data_path(&self) -> PathBuf {
        let temp_dir = self.temp_dir.as_ref().unwrap().path();

        temp_dir.join("e2e-output-data.txt")
    }

    pub async fn execute<I, S>(&self, cmd: I) -> ExecuteCommandResult
    where
        I: IntoIterator<Item = S>,
        S: AsRef<ffi::OsStr>,
    {
        self.execute_impl(cmd, None::<Vec<u8>>, None).await
    }

    pub async fn execute_with_input<I, S, T>(&self, cmd: I, input: T) -> ExecuteCommandResult
    where
        I: IntoIterator<Item = S>,
        S: AsRef<ffi::OsStr>,
        T: Into<Vec<u8>>,
    {
        self.execute_impl(cmd, Some(input), None).await
    }

    pub async fn execute_with_env<I, S>(
        &self,
        cmd: I,
        env_vars: Vec<(&ffi::OsStr, &ffi::OsStr)>,
    ) -> ExecuteCommandResult
    where
        I: IntoIterator<Item = S>,
        S: AsRef<ffi::OsStr>,
    {
        self.execute_impl(cmd, None::<Vec<u8>>, Some(env_vars))
            .await
    }

    async fn execute_impl<I, S, T>(
        &self,
        cmd: I,
        maybe_input: Option<T>,
        maybe_env: Option<Vec<(&ffi::OsStr, &ffi::OsStr)>>,
    ) -> ExecuteCommandResult
    where
        I: IntoIterator<Item = S>,
        S: AsRef<ffi::OsStr>,
        T: Into<Vec<u8>>,
    {
        let mut command = assert_cmd::Command::cargo_bin("kamu-cli").unwrap();

        if let Some(env_vars) = maybe_env {
            for (name, value) in env_vars {
                command.env(name, value);
            }
        };

        command.env("RUST_LOG", "info,sqlx=debug");

        command.arg("-v");
        command.arg("--no-color");

        command.current_dir(self.workspace_path.clone());

        if let Some(system_time) = &self.system_time {
            let system_time = system_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            command.args(["--system-time", system_time.as_str()]);
        }

        if let Some(account) = &self.account {
            command.args(["--account", account.as_str()]);
        }

        command.args(cmd);

        if let Some(input) = maybe_input {
            command.write_stdin(input);
        }

        tokio::task::spawn_blocking(move || command.assert())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct NewWorkspaceOptions {
    pub is_multi_tenant: bool,
    pub kamu_config: Option<String>,
    pub env_vars: Vec<(String, String)>,
    pub account: Option<odf::AccountName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
