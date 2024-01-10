// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use container_runtime::ContainerRuntime;
use internal_error::*;

use super::{CLIError, Command};
use crate::output::*;

///////////////////////////////////////////////////////////////////////////////

pub struct SystemInfoCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
    workspace_root_dir: String,
    container_runtime: Arc<ContainerRuntime>,
}

impl SystemInfoCommand {
    pub fn new<S>(
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        workspace_root_dir: String,
        output_format: Option<S>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
            container_runtime,
            workspace_root_dir,
            output_format: output_format.map(|s| s.into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemInfoCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        write_output(
            SystemInfo::collect(
                self.container_runtime.clone(),
                self.workspace_root_dir.clone(),
            )
            .await,
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct VersionCommand {
    output_config: Arc<OutputConfig>,
    workspace_root_dir: String,
    output_format: Option<String>,
}

impl VersionCommand {
    pub fn new<S>(
        output_config: Arc<OutputConfig>,
        workspace_root_dir: String,
        output_format: Option<S>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
            workspace_root_dir,
            output_format: output_format.map(|s| s.into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for VersionCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        write_output(
            BuildInfo::collect(self.workspace_root_dir.clone()),
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

fn write_output<T: serde::Serialize>(
    value: T,
    output_config: &OutputConfig,
    output_format: Option<impl AsRef<str>>,
) -> Result<(), InternalError> {
    let output_format = if let Some(fmt) = &output_format {
        fmt.as_ref()
    } else {
        if output_config.is_tty {
            "shell"
        } else {
            "json"
        }
    };

    // TODO: Generalize this code in output config, just like we do for tabular
    // output
    match output_format {
        "json" => serde_json::to_writer_pretty(std::io::stdout(), &value).int_err()?,
        "shell" | "yaml" | _ => serde_yaml::to_writer(std::io::stdout(), &value).int_err()?,
    }

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub build: BuildInfo,
    pub additional: AdditionalInfo,
}

impl SystemInfo {
    pub async fn collect(
        container_runtime: Arc<ContainerRuntime>,
        workspace_root_dir: String,
    ) -> Self {
        Self {
            build: BuildInfo::collect(workspace_root_dir),
            additional: AdditionalInfo::collect(container_runtime).await,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub app_version: &'static str,
    pub build_timestamp: Option<&'static str>,
    pub git_describe: Option<&'static str>,
    pub git_sha: Option<&'static str>,
    pub git_commit_date: Option<&'static str>,
    pub git_branch: Option<&'static str>,
    pub rustc_semver: Option<&'static str>,
    pub rustc_channel: Option<&'static str>,
    pub rustc_host_triple: Option<&'static str>,
    pub rustc_commit_sha: Option<&'static str>,
    pub cargo_target_triple: Option<&'static str>,
    pub cargo_features: Option<&'static str>,
    pub cargo_opt_level: Option<&'static str>,
    pub workspace_dir: String,
}

impl BuildInfo {
    pub fn collect(workspace_root_dir: String) -> Self {
        Self {
            app_version: env!("CARGO_PKG_VERSION"),
            build_timestamp: option_env!("VERGEN_BUILD_TIMESTAMP"),
            git_describe: option_env!("VERGEN_GIT_DESCRIBE"),
            git_sha: option_env!("VERGEN_GIT_SHA"),
            git_commit_date: option_env!("VERGEN_GIT_COMMIT_DATE"),
            git_branch: option_env!("VERGEN_GIT_BRANCH"),
            rustc_semver: option_env!("VERGEN_RUSTC_SEMVER"),
            rustc_channel: option_env!("VERGEN_RUSTC_CHANNEL"),
            rustc_host_triple: option_env!("VERGEN_RUSTC_HOST_TRIPLE"),
            rustc_commit_sha: option_env!("VERGEN_RUSTC_COMMIT_HASH"),
            cargo_target_triple: option_env!("VERGEN_CARGO_TARGET_TRIPLE"),
            cargo_features: option_env!("VERGEN_CARGO_FEATURES"),
            cargo_opt_level: option_env!("VERGEN_CARGO_OPT_LEVEL"),
            workspace_dir: workspace_root_dir,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdditionalInfo {
    pub container_version: String,
}

impl AdditionalInfo {
    pub async fn collect(container_runtime: Arc<ContainerRuntime>) -> Self {
        let container_version_output = match container_runtime
            .custom_cmd("--version".to_string())
            .output()
            .await
        {
            Ok(container_info) => String::from_utf8(container_info.stdout).unwrap(),
            Err(_) => "".to_string(),
        };

        Self {
            container_version: container_version_output,
        }
    }
}
