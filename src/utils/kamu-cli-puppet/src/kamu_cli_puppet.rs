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
        let options = NewWorkspaceOptions::builder()
            .is_multi_tenant(true)
            .kamu_config(
                indoc::indoc!(
                    r#"
                    kind: CLIConfig
                    version: 1
                    content:
                      users:
                        predefined:
                          - accountName: e2e-user
                            password: e2e-user
                            email: e2e-user@example.com
                    "#
                )
                .into(),
            )
            .account(odf::AccountName::new_unchecked("e2e-user"))
            .build();

        Self::new_workspace_tmp_with(options).await
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
            // TODO: Reconsider setting global vars in tests
            unsafe {
                std::env::set_var(env_name, env_value);
            }
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

    pub async fn execute<CmdIt, CmdItem>(&self, cmd: CmdIt) -> ExecuteCommandResult
    where
        CmdIt: IntoIterator<Item = CmdItem>,
        CmdItem: AsRef<ffi::OsStr>,
    {
        let options = ExecuteOptions::<Vec<u8>, Vec<(&str, &str)>, &str>::default();
        self.execute_impl(cmd, options).await
    }

    pub async fn execute_with_input<CmdIt, CmdItem, InputIt>(
        &self,
        cmd: CmdIt,
        input: InputIt,
    ) -> ExecuteCommandResult
    where
        CmdIt: IntoIterator<Item = CmdItem>,
        CmdItem: AsRef<ffi::OsStr>,
        InputIt: Into<Vec<u8>>,
    {
        let options = ExecuteOptions::<_, Vec<(&str, &str)>, _>::builder()
            .input(input)
            .build();
        self.execute_impl(cmd, options).await
    }

    pub async fn execute_with_env<CmdIt, CmdItem, EnvIt, EnvItem>(
        &self,
        cmd: CmdIt,
        env_vars: EnvIt,
    ) -> ExecuteCommandResult
    where
        CmdIt: IntoIterator<Item = CmdItem>,
        CmdItem: AsRef<ffi::OsStr>,
        EnvIt: IntoIterator<Item = (EnvItem, EnvItem)>,
        EnvItem: AsRef<ffi::OsStr>,
    {
        let options = ExecuteOptions::<Vec<u8>, _, _>::builder()
            .env(env_vars)
            .build();
        self.execute_impl(cmd, options).await
    }

    async fn execute_impl<CmdIt, CmdItem, InputIt, EnvIt, EnvItem>(
        &self,
        cmd: CmdIt,
        options: ExecuteOptions<InputIt, EnvIt, EnvItem>,
    ) -> ExecuteCommandResult
    where
        CmdIt: IntoIterator<Item = CmdItem>,
        CmdItem: AsRef<ffi::OsStr>,
        InputIt: Into<Vec<u8>>,
        EnvIt: IntoIterator<Item = (EnvItem, EnvItem)>,
        EnvItem: AsRef<ffi::OsStr>,
    {
        let mut command = assert_cmd::Command::cargo_bin("kamu-cli").unwrap();

        if let Some(env) = options.env {
            for (name, value) in env {
                command.env(name, value);
            }
        }

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

        command.args(["--password-hashing-mode", "testing"]);

        command.args(cmd);

        if let Some(input) = options.input {
            command.write_stdin(input);
        }

        tokio::task::spawn_blocking(move || command.assert())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, bon::Builder)]
pub struct NewWorkspaceOptions {
    pub is_multi_tenant: bool,
    pub kamu_config: Option<String>,
    #[builder(default)]
    pub env_vars: Vec<(String, String)>,
    pub account: Option<odf::AccountName>,
}

#[derive(bon::Builder)]
struct ExecuteOptions<InputIt, EnvIt, EnvItem>
where
    InputIt: Into<Vec<u8>>,
    EnvIt: IntoIterator<Item = (EnvItem, EnvItem)>,
    EnvItem: AsRef<ffi::OsStr>,
{
    input: Option<InputIt>,
    env: Option<EnvIt>,
}

impl<I, E, S> Default for ExecuteOptions<I, E, S>
where
    I: Into<Vec<u8>>,
    E: IntoIterator<Item = (S, S)>,
    S: AsRef<ffi::OsStr>,
{
    fn default() -> Self {
        ExecuteOptions {
            input: None,
            env: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
