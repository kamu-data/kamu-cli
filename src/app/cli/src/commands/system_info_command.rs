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

use super::{CLIError, Command};
use crate::output::*;
use crate::WorkspaceService;

///////////////////////////////////////////////////////////////////////////////

pub struct SystemInfoCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
}

impl SystemInfoCommand {
    pub fn new<S>(output_config: Arc<OutputConfig>, output_format: Option<S>) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
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
            SystemInfo::collect().await,
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct VersionCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
}

impl VersionCommand {
    pub fn new<S>(output_config: Arc<OutputConfig>, output_format: Option<S>) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
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
            BuildInfo::collect(),
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub build: BuildInfo,
    pub additional: AdditionalInfo,
}

impl SystemInfo {
    pub async fn collect() -> Self {
        Self {
            build: BuildInfo::collect(),
            additional: AdditionalInfo::collect().await,
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
    pub workspace_dir: Option<String>,
}

impl BuildInfo {
    pub fn collect() -> Self {
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
            workspace_dir: WorkspaceService::find_workspace()
                .root_dir
                .to_str()
                .map(String::from),
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdditionalInfo {
    pub container_version: String,
}

impl AdditionalInfo {
    pub async fn collect() -> Self {
        let container_runtime = ContainerRuntime::default();
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
