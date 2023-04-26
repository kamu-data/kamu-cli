// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use crate::{WorkspaceService, WorkspaceUpgradeError};
use std::sync::Arc;

pub struct UpgradeWorkspaceCommand {
    workspace_svc: Arc<WorkspaceService>,
}

impl UpgradeWorkspaceCommand {
    pub fn new(workspace_svc: Arc<WorkspaceService>) -> Self {
        Self { workspace_svc }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for UpgradeWorkspaceCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        if !self.workspace_svc.is_upgrade_needed()? {
            eprintln!("{}", console::style("Workspace is up-to-date").yellow());
            Ok(())
        } else {
            match self.workspace_svc.upgrade() {
                Ok(res) => {
                    eprintln!(
                        "{}",
                        console::style(format!(
                            "Successfully upgraded workspace from version {} to {}",
                            res.prev_version, res.new_version
                        ))
                        .green()
                        .bold()
                    );
                    Ok(())
                }
                Err(WorkspaceUpgradeError::FutureVersion(err)) => {
                    Err(CLIError::usage_error_from(err))
                }
                Err(err) => Err(CLIError::critical(err)),
            }
        }
    }
}
