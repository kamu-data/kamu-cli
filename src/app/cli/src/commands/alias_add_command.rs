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
use opendatafabric::*;

use super::{CLIError, Command};

pub struct AliasAddCommand {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset: DatasetRef,
    alias: DatasetRefRemote,
    pull: bool,
    push: bool,
}

impl AliasAddCommand {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset: DatasetRef,
        alias: DatasetRefRemote,
        pull: bool,
        push: bool,
    ) -> Self {
        Self {
            remote_repo_reg,
            remote_alias_reg,
            dataset,
            alias,
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

        if let DatasetRefRemote::Alias(alias) = &self.alias {
            self.remote_repo_reg
                .get_repository(&alias.repo_name)
                .map_err(CLIError::failure)?;
        }

        let mut aliases = self
            .remote_alias_reg
            .get_remote_aliases(&self.dataset)
            .await
            .map_err(CLIError::failure)?;

        if self.pull {
            if aliases.add(&self.alias, RemoteAliasKind::Pull).await? {
                eprintln!(
                    "{}: {} ({})",
                    console::style("Added").green(),
                    self.alias,
                    "pull"
                );
            }
        }

        if self.push {
            if aliases.add(&self.alias, RemoteAliasKind::Push).await? {
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
