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
use opendatafabric::RepositoryBuf;

use std::{convert::TryFrom, sync::Arc};

pub struct RepositoryDeleteCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    names: Vec<String>,
    all: bool,
    no_confirmation: bool,
}

impl RepositoryDeleteCommand {
    pub fn new<I, S>(
        metadata_repo: Arc<dyn MetadataRepository>,
        names: I,
        all: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            metadata_repo: metadata_repo,
            names: names.map(|s| s.as_ref().to_owned()).collect(),
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
        let repo_ids: Vec<RepositoryBuf> = if self.all {
            self.metadata_repo.get_all_repositories().collect()
        } else {
            self.names
                .iter()
                .map(|s| RepositoryBuf::try_from(s.as_str()).unwrap())
                .collect()
        };

        if repo_ids.is_empty() {
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
                repo_ids
                    .iter()
                    .map(|id| id.as_str())
                    .collect::<Vec<&str>>()
                    .join(", "),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for id in repo_ids.iter() {
            self.metadata_repo.delete_repository(id)?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} repository(s)", repo_ids.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}
