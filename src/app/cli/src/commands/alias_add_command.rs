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

pub struct AliasAddCommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset_ref: odf::DatasetRef,
    alias: odf::DatasetRefRemote,
    pull: bool,
    push: bool,
}

impl AliasAddCommand {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset: odf::DatasetRef,
        alias: odf::DatasetRefRemote,
        pull: bool,
        push: bool,
    ) -> Self {
        Self {
            dataset_registry,
            remote_repo_reg,
            remote_alias_reg,
            dataset_ref: dataset,
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

        if let odf::DatasetRefRemote::Alias(alias) = &self.alias {
            self.remote_repo_reg
                .get_repository(&alias.repo_name)
                .map_err(CLIError::failure)?;
        }

        let dataset_handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&self.dataset_ref)
            .await
            .map_err(CLIError::failure)?;

        let mut aliases = self
            .remote_alias_reg
            .get_remote_aliases(&dataset_handle)
            .await
            .map_err(CLIError::failure)?;

        if self.pull && aliases.add(&self.alias, RemoteAliasKind::Pull).await? {
            eprintln!("{}: {} (pull)", console::style("Added").green(), self.alias);
        }

        if self.push && aliases.add(&self.alias, RemoteAliasKind::Push).await? {
            eprintln!("{}: {} (push)", console::style("Added").green(), self.alias);
        }

        Ok(())
    }
}
