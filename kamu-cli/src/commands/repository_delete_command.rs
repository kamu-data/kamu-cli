// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::RepositoryName;

use std::sync::Arc;

pub struct RepositoryDeleteCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    names: Vec<RepositoryName>,
    all: bool,
    no_confirmation: bool,
}

impl RepositoryDeleteCommand {
    pub fn new<I, N>(
        metadata_repo: Arc<dyn MetadataRepository>,
        names: I,
        all: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: Iterator<Item = N>,
        N: TryInto<RepositoryName>,
        <N as TryInto<RepositoryName>>::Error: std::fmt::Debug,
    {
        Self {
            metadata_repo: metadata_repo,
            names: names.map(|s| s.try_into().unwrap()).collect(),
            all: all,
            no_confirmation: no_confirmation,
        }
    }

    fn prompt_yes_no(&self, msg: &str) -> bool {
        use read_input::prelude::*;

        let answer: String = input()
            .repeat_msg(msg)
            .default("n".to_owned())
            .add_test(|v| match v.as_ref() {
                "n" | "N" | "no" | "y" | "Y" | "yes" => true,
                _ => false,
            })
            .get();

        match answer.as_ref() {
            "n" | "N" | "no" => false,
            _ => true,
        }
    }
}

impl Command for RepositoryDeleteCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let repo_names: Vec<_> = if self.all {
            self.metadata_repo.get_all_repositories().collect()
        } else {
            self.names.clone()
        };

        if repo_names.is_empty() {
            return Err(CLIError::usage_error(
                "Specify a repository or use --all flag",
            ));
        }

        let confirmed = if self.no_confirmation {
            true
        } else {
            self.prompt_yes_no(&format!(
                "{}: {}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to delete following repository(s)").yellow(),
                repo_names
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", "),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for name in &repo_names {
            self.metadata_repo.delete_repository(name)?;
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
