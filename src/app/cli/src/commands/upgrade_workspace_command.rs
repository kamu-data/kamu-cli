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
use crate::{WorkspaceService, WorkspaceUpgradeError};

#[dill::component]
#[dill::interface(dyn Command)]
pub struct UpgradeWorkspaceCommand {
    workspace_svc: Arc<WorkspaceService>,
}

#[async_trait::async_trait(?Send)]
impl Command for UpgradeWorkspaceCommand {
    async fn run(&self) -> Result<(), CLIError> {
        if !self.workspace_svc.is_upgrade_needed()? {
            eprintln!("{}", console::style("Workspace is up-to-date").yellow());
            Ok(())
        } else {
            match self.workspace_svc.upgrade().await {
                Ok(res) => {
                    eprintln!(
                        "{} {} {} {}",
                        console::style("Successfully upgraded workspace from version")
                            .green()
                            .bold(),
                        res.prev_version,
                        console::style("to").green().bold(),
                        res.new_version,
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
