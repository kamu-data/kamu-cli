use super::{Command, Error};
use kamu::infra::*;

use std::fs;

pub struct InitCommand {
    workspace_layout: WorkspaceLayout,
}

impl InitCommand {
    pub fn new<'a>(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
        }
    }
}

impl Command for InitCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        if self.workspace_layout.kamu_root_dir.is_dir() {
            return Err(Error::AlreadyInWorkspace);
        }

        fs::create_dir_all(&self.workspace_layout.datasets_dir)?;
        fs::create_dir_all(&self.workspace_layout.remotes_dir)?;
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
