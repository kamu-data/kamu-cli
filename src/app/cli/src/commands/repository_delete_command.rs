// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;

use super::{CLIError, Command};
use crate::Interact;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RepositoryDeleteCommand {
    interact: Arc<Interact>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    names: Vec<odf::RepoName>,
    all: bool,
}

impl RepositoryDeleteCommand {
    pub fn new<I>(
        interact: Arc<Interact>,
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        names: I,
        all: bool,
    ) -> Self
    where
        I: IntoIterator<Item = odf::RepoName>,
    {
        Self {
            interact,
            remote_repo_reg,
            names: names.into_iter().collect(),
            all,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for RepositoryDeleteCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        match (self.names.as_slice(), self.all) {
            ([], false) => Err(CLIError::usage_error("Specify repository(s) or pass --all")),
            ([], true) => Ok(()),
            ([_head, ..], false) => Ok(()),
            ([_head, ..], true) => Err(CLIError::usage_error(
                "You can either specify repository(s) or pass --all",
            )),
        }
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let repo_names: Vec<_> = if self.all {
            self.remote_repo_reg.get_all_repositories().collect()
        } else {
            for name in &self.names {
                self.remote_repo_reg
                    .get_repository(name)
                    .map_err(CLIError::failure)?;
            }
            self.names.clone()
        };

        if repo_names.is_empty() {
            eprintln!(
                "{}",
                console::style("There are no repositories to delete").yellow()
            );
            return Ok(());
        }

        self.interact.require_confirmation(format!(
            "{}: {}",
            console::style("You are about to delete following repository(s)").yellow(),
            itertools::join(&repo_names, ", "),
        ))?;

        for name in &repo_names {
            self.remote_repo_reg
                .delete_repository(name)
                .map_err(CLIError::failure)?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} repository(s)", repo_names.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
