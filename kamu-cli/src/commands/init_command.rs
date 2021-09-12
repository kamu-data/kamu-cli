// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::AlreadyInWorkspace;

use super::{CLIError, Command};
use kamu::infra::*;

use std::{fs, sync::Arc};

pub struct InitCommand {
    workspace_layout: Arc<WorkspaceLayout>,
}

impl InitCommand {
    pub fn new<'a>(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }
}

impl Command for InitCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), CLIError> {
        if self.workspace_layout.kamu_root_dir.is_dir() {
            return Err(CLIError::usage_error_from(AlreadyInWorkspace));
        }

        fs::create_dir_all(&self.workspace_layout.datasets_dir)?;
        fs::create_dir_all(&self.workspace_layout.repos_dir)?;
        fs::create_dir_all(&self.workspace_layout.run_info_dir)?;
        fs::create_dir_all(&self.workspace_layout.local_volume_dir)?;

        eprintln!(
            "{}",
            console::style("Initialized an empty workspace")
                .green()
                .bold()
        );
        Ok(())
    }
}
