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
use opendatafabric::{DatasetID, DatasetRefBuf};

use std::{convert::TryFrom, sync::Arc};

pub struct AliasAddCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset: String,
    alias: String,
    pull: bool,
    push: bool,
}

impl AliasAddCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset: String,
        alias: String,
        pull: bool,
        push: bool,
    ) -> Self {
        Self {
            metadata_repo,
            dataset,
            alias,
            pull,
            push,
        }
    }
}

impl Command for AliasAddCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if !self.pull && !self.push {
            return Err(CLIError::usage_error(
                "Specify either --pull or --push or both",
            ));
        }

        let dataset_id = DatasetID::try_from(&self.dataset).unwrap();
        let remote_ref = DatasetRefBuf::try_from(self.alias.clone()).unwrap();

        let repo_id = remote_ref.repository().ok_or(CLIError::usage_error(
            "Alias should contain a repository part",
        ))?;

        self.metadata_repo.get_repository(repo_id)?;
        let mut aliases = self.metadata_repo.get_remote_aliases(dataset_id)?;

        if self.pull {
            if aliases.add(remote_ref.clone(), RemoteAliasKind::Pull)? {
                eprintln!(
                    "{}: {} ({})",
                    console::style("Added").green(),
                    remote_ref,
                    "pull"
                );
            }
        }

        if self.push {
            if aliases.add(remote_ref.clone(), RemoteAliasKind::Push)? {
                eprintln!(
                    "{}: {} ({})",
                    console::style("Added").green(),
                    remote_ref,
                    "push"
                );
            }
        }

        Ok(())
    }
}
