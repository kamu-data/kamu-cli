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
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset: DatasetRefLocal,
    alias: DatasetRefRemote,
    pull: bool,
    push: bool,
}

impl AliasAddCommand {
    pub fn new<R, N>(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset: R,
        alias: N,
        pull: bool,
        push: bool,
    ) -> Self
    where
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,
        N: TryInto<DatasetRefRemote>,
        <N as TryInto<DatasetRefRemote>>::Error: std::fmt::Debug,
    {
        Self {
            remote_repo_reg,
            remote_alias_reg,
            dataset: dataset.try_into().unwrap(),
            alias: alias.try_into().unwrap(),
            pull,
            push,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AliasAddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if !self.pull && !self.push {
            return Err(CLIError::usage_error(
                "Specify either --pull or --push or both",
            ));
        }

        if let DatasetRefRemote::RemoteName(name) = &self.alias {
            self.remote_repo_reg
                .get_repository(name.repository())
                .map_err(CLIError::failure)?;
        }

        let mut aliases = self
            .remote_alias_reg
            .get_remote_aliases(&self.dataset)
            .await
            .map_err(CLIError::failure)?;

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
