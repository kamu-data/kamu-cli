// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::TenancyConfig;

use super::{CLIError, Command};
use crate::{AlreadyInWorkspace, OutputConfig, WorkspaceLayout};

pub struct InitCommand {
    output_config: Arc<OutputConfig>,
    workspace_layout: Arc<WorkspaceLayout>,
    exists_ok: bool,
    tenancy_config: TenancyConfig,
}

impl InitCommand {
    pub fn new(
        output_config: Arc<OutputConfig>,
        workspace_layout: Arc<WorkspaceLayout>,
        exists_ok: bool,
        tenancy_config: TenancyConfig,
    ) -> Self {
        Self {
            output_config,
            workspace_layout,
            exists_ok,
            tenancy_config,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for InitCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if self.workspace_layout.root_dir.is_dir() {
            return if self.exists_ok {
                if !self.output_config.quiet {
                    eprintln!("{}", console::style("Workspace already exists").yellow());
                }
                Ok(())
            } else {
                Err(CLIError::usage_error_from(AlreadyInWorkspace))
            };
        }

        WorkspaceLayout::create(&self.workspace_layout.root_dir, self.tenancy_config)?;

        // TODO, write a workspace config

        if !self.output_config.quiet {
            eprintln!(
                "{}",
                console::style(match self.tenancy_config {
                    TenancyConfig::MultiTenant => "Initialized an empty multi-tenant workspace",
                    TenancyConfig::SingleTenant => "Initialized an empty workspace",
                })
                .green()
                .bold()
            );
        }

        Ok(())
    }
}
