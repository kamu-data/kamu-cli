// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use crate::{AlreadyInWorkspace, OutputConfig, WorkspaceLayout};

pub struct InitCommand {
    output_config: Arc<OutputConfig>,
    workspace_layout: Arc<WorkspaceLayout>,
    exists_ok: bool,
    multi_tenant: bool,
}

impl InitCommand {
    pub fn new(
        output_config: Arc<OutputConfig>,
        workspace_layout: Arc<WorkspaceLayout>,
        exists_ok: bool,
        multi_tenant: bool,
    ) -> Self {
        Self {
            output_config,
            workspace_layout,
            exists_ok,
            multi_tenant,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for InitCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn validate_args(&self) -> Result<(), CLIError> {
        if !self.workspace_layout.root_dir.is_dir() {
            return Ok(());
        }

        if self.exists_ok {
            if !self.output_config.quiet {
                eprintln!("{}", console::style("Workspace already exists").yellow());
            }

            Ok(())
        } else {
            Err(CLIError::usage_error_from(AlreadyInWorkspace))
        }
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        WorkspaceLayout::create(&self.workspace_layout.root_dir, self.multi_tenant)?;

        // TODO, write a workspace config

        if !self.output_config.quiet {
            eprintln!(
                "{}",
                console::style(if self.multi_tenant {
                    "Initialized an empty multi-tenant workspace"
                } else {
                    "Initialized an empty workspace"
                })
                .green()
                .bold()
            );
        }

        Ok(())
    }
}
