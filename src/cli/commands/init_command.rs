use super::{Command, Error};
use kamu::infra::*;

use std::fs;
use std::io::prelude::*;

pub struct InitCommand<'a> {
    workspace_layout: &'a WorkspaceLayout,
}

impl InitCommand<'_> {
    pub fn new<'a>(workspace_layout: &'a WorkspaceLayout) -> InitCommand<'a> {
        InitCommand {
            workspace_layout: workspace_layout,
        }
    }
}

impl Command for InitCommand<'_> {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        if self.workspace_layout.kamu_root_dir.is_dir() {
            return Err(Error::AlreadyInWorkspace);
        }

        fs::create_dir_all(&self.workspace_layout.datasets_dir)?;
        fs::create_dir_all(&self.workspace_layout.remotes_dir)?;

        let gitignore_path = self.workspace_layout.kamu_root_dir.join(".gitignore");
        let mut gitignore = fs::File::create(gitignore_path)?;
        writeln!(gitignore, "/config")?;
        gitignore.sync_all()?;

        eprintln!("Initialized an empty workspace");
        Ok(())
    }
}
