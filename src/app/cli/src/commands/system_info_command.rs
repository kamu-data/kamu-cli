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
use observability::build_info::BuildInfo;
use serde_json::Value;

use super::{CLIError, Command};
use crate::output::*;
use crate::{WorkspaceService, WorkspaceVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SystemInfoOutputFormat {
    Shell,
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SystemInfoCommand {
    build_info: observability::build_info::BuildInfo,
    output_config: Arc<OutputConfig>,
    workspace_svc: Arc<WorkspaceService>,
    container_runtime: Arc<ContainerRuntime>,

    #[dill::component(explicit)]
    output_format: Option<SystemInfoOutputFormat>,
}

impl SystemInfoCommand {
    pub async fn collect(&self) -> SystemInfo {
        SystemInfo::collect(
            self.build_info.clone(),
            &self.container_runtime,
            &self.workspace_svc,
        )
        .await
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemInfoCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let system_info = self.collect().await;
        write_output(system_info, &self.output_config, self.output_format)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct VersionCommand {
    build_info: BuildInfo,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    output_format: Option<SystemInfoOutputFormat>,
}

#[async_trait::async_trait(?Send)]
impl Command for VersionCommand {
    async fn run(&self) -> Result<(), CLIError> {
        write_output(&self.build_info, &self.output_config, self.output_format)?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn write_output<T: serde::Serialize>(
    value: T,
    output_config: &OutputConfig,
    output_format: Option<SystemInfoOutputFormat>,
) -> Result<(), InternalError> {
    let output_format = output_format.unwrap_or(if output_config.is_tty {
        SystemInfoOutputFormat::Shell
    } else {
        SystemInfoOutputFormat::Json
    });

    // TODO: Generalize this code in output config, just like we do for tabular
    // output
    match output_format {
        SystemInfoOutputFormat::Json => {
            serde_json::to_writer_pretty(std::io::stdout(), &value).int_err()?;
        }
        SystemInfoOutputFormat::Yaml | SystemInfoOutputFormat::Shell => {
            serde_yaml::to_writer(std::io::stdout(), &value).int_err()?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub build: BuildInfo,
    pub workspace: WorkspaceInfo,
    pub container_runtime: ContainerRuntimeInfo,
}

impl SystemInfo {
    pub async fn collect(
        build: BuildInfo,
        container_runtime_svc: &ContainerRuntime,
        workspace_svc: &WorkspaceService,
    ) -> Self {
        Self {
            build,
            workspace: WorkspaceInfo::collect(workspace_svc),
            container_runtime: ContainerRuntimeInfo::collect(container_runtime_svc).await,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceInfo {
    pub version: u32,
    pub root_dir: String,
}

impl WorkspaceInfo {
    pub fn collect(workspace_svc: &WorkspaceService) -> Self {
        if !workspace_svc.is_in_workspace() {
            return Self {
                version: 0,
                root_dir: String::new(),
            };
        }

        let root_dir = match workspace_svc.layout() {
            Some(wl) => wl.root_dir.to_str().map(String::from).unwrap(),
            None => String::new(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRuntimeInfo {
    pub info: Value,
}

impl ContainerRuntimeInfo {
    pub async fn collect(container_runtime: &ContainerRuntime) -> Self {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
