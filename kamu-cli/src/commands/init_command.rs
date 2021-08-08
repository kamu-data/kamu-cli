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
            return Err(CLIError::AlreadyInWorkspace);
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
