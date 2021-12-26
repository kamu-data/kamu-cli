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
use opendatafabric::*;

use std::sync::Arc;

pub struct AliasAddCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset: DatasetRefLocal,
    alias: RemoteDatasetName,
    pull: bool,
    push: bool,
}

impl AliasAddCommand {
    pub fn new<R, N>(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset: R,
        alias: N,
        pull: bool,
        push: bool,
    ) -> Self
    where
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,
        N: TryInto<RemoteDatasetName>,
        <N as TryInto<RemoteDatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            metadata_repo,
            dataset: dataset.try_into().unwrap(),
            alias: alias.try_into().unwrap(),
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

        self.metadata_repo
            .get_repository(&self.alias.repository())?;
        let mut aliases = self.metadata_repo.get_remote_aliases(&self.dataset)?;

        if self.pull {
            if aliases.add(&self.alias, RemoteAliasKind::Pull)? {
                eprintln!(
                    "{}: {} ({})",
                    console::style("Added").green(),
                    self.alias,
                    "pull"
                );
            }
        }

        if self.push {
            if aliases.add(&self.alias, RemoteAliasKind::Push)? {
                eprintln!(
                    "{}: {} ({})",
                    console::style("Added").green(),
                    self.alias,
                    "push"
                );
            }
        }

        Ok(())
    }
}
