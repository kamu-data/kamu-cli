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
use crate::{AlreadyInWorkspace, WorkspaceLayout};

pub struct InitCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    exists_ok: bool,
    multi_tenant: bool,
}

impl InitCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        exists_ok: bool,
        multi_tenant: bool,
    ) -> Self {
        Self {
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

    async fn run(&mut self) -> Result<(), CLIError> {
        if self.workspace_layout.root_dir.is_dir() {
            return if self.exists_ok {
                eprintln!("{}", console::style("Workspace already exists").yellow());
                Ok(())
            } else {
                Err(CLIError::usage_error_from(AlreadyInWorkspace))
            };
        }

        WorkspaceLayout::create(&self.workspace_layout.root_dir, self.multi_tenant)?;

        // TODO, write a workspace config

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
        Ok(())
    }
}
