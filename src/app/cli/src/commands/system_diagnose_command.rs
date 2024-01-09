// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use container_runtime::{ContainerRuntime, ContainerRuntimeType};

use super::{CLIError, Command};
use crate::output::*;
use crate::WorkspaceService;

pub struct SystemDiagnoseCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
}

impl SystemDiagnoseCommand {
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
impl Command for SystemDiagnoseCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        write_output(
            SystemDiagnose::check().await,
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemDiagnose {
    pub run_check: RunCheck,
}

impl SystemDiagnose {
    pub async fn check() -> Self {
        Self {
            run_check: RunCheck::collect().await,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCheck {
    pub container_runtime_type: ContainerRuntimeType,
    pub is_installed: bool,
    pub is_rootless: bool,
    pub workspace_dir: Option<String>,
    pub is_workspace_consitence: bool,
}

impl RunCheck {
    pub async fn collect() -> Self {
        let container_runtime = ContainerRuntime::default();
        let is_container_command_err = container_runtime
            .custom_cmd("--version".to_string())
            .output()
            .await
            .is_err();

        let workspace_layout = WorkspaceService::find_workspace();
        let current_workspace = WorkspaceService::new(Arc::new(workspace_layout.clone()));
        let container_info = container_runtime
            .custom_cmd(r"info".to_string())
            .output()
            .await
            .unwrap();

        Self {
            container_runtime_type: container_runtime.config.runtime,
            is_installed: !is_container_command_err,
            is_rootless: String::from_utf8(container_info.stdout)
                .unwrap()
                .contains("rootless: true"),
            workspace_dir: workspace_layout.root_dir.to_str().map(String::from),
            is_workspace_consitence: current_workspace.is_in_workspace()
                && current_workspace.is_upgrade_needed().unwrap_or(false),
        }
    }
}
