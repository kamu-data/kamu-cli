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
use serde_json::Value;

use super::{CLIError, Command};
use crate::output::*;
use crate::{WorkspaceService, WorkspaceVersion};

///////////////////////////////////////////////////////////////////////////////

pub struct SystemInfoCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
    workspace_svc: Arc<WorkspaceService>,
    container_runtime: Arc<ContainerRuntime>,
}

impl SystemInfoCommand {
    pub fn new<S>(
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        workspace_svc: Arc<WorkspaceService>,
        output_format: Option<S>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
            container_runtime,
            workspace_svc,
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
            SystemInfo::collect(self.container_runtime.clone(), self.workspace_svc.clone()).await,
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

fn write_output<T: serde::Serialize>(
    value: T,
    output_config: &OutputConfig,
    output_format: Option<impl AsRef<str>>,
) -> Result<(), InternalError> {
    let output_format = if let Some(fmt) = &output_format {
        fmt.as_ref()
    } else if output_config.is_tty {
        "shell"
    } else {
        "json"
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
    pub workspace: WorkspaceInfo,
    pub container_runtime: ContainerRuntimeInfo,
}

impl SystemInfo {
    pub async fn collect(
        container_runtime_svc: Arc<ContainerRuntime>,
        workspace_svc: Arc<WorkspaceService>,
    ) -> Self {
        Self {
            build: BuildInfo::collect(),
            workspace: WorkspaceInfo::collect(workspace_svc),
            container_runtime: ContainerRuntimeInfo::collect(container_runtime_svc).await,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

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
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceInfo {
    pub version: u32,
    pub root_dir: String,
}

impl WorkspaceInfo {
    pub fn collect(workspace_svc: Arc<WorkspaceService>) -> Self {
        if !workspace_svc.is_in_workspace() {
            return Self {
                version: 0,
                root_dir: "".to_string(),
            };
        }

        let root_dir = match workspace_svc.layout() {
            Some(wl) => wl.root_dir.to_str().map(String::from).unwrap(),
            None => "".to_string(),
        };

        let workspace_version = workspace_svc
            .workspace_version()
            .unwrap_or_default()
            .unwrap_or(WorkspaceVersion::Unknown(0));

        Self {
            version: workspace_version.into(),
            root_dir,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRuntimeInfo {
    pub info: Value,
}

impl ContainerRuntimeInfo {
    pub async fn collect(container_runtime: Arc<ContainerRuntime>) -> Self {
        let container_info_output = match container_runtime.info().output().await {
            Ok(container_info) => {
                if container_info.status.success() {
                    match serde_json::from_slice(&container_info.stdout) {
                        Ok(res) => res,
                        Err(_) => {
                            serde_json::json!({"error": "Unable to parse container runtime info result"})
                        }
                    }
                } else {
                    serde_json::json!({"error": "Unable to run container runtime info command"})
                }
            }
            Err(_) => {
                serde_json::json!({"error": "Unable to fetch container runtime info"})
            }
        };

        Self {
            info: container_info_output,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
