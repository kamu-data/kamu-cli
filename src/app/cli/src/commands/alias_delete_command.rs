// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::StreamExt;
use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};

pub struct AliasDeleteCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset: Option<DatasetRef>,
    alias: Option<DatasetRefRemote>,
    all: bool,
    pull: bool,
    push: bool,
}

impl AliasDeleteCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset: Option<DatasetRef>,
        alias: Option<DatasetRefRemote>,
        all: bool,
        pull: bool,
        push: bool,
    ) -> Self {
        Self {
            dataset_repo,
            remote_alias_reg,
            dataset,
            alias,
            all,
            pull,
            push,
        }
    }

    async fn delete_dataset_alias(&self) -> Result<usize, CLIError> {
        let mut aliases = self
            .remote_alias_reg
            .get_remote_aliases(self.dataset.as_ref().unwrap())
            .await
            .map_err(CLIError::failure)?;

        let mut count = 0;

        if self.all {
            count += aliases.clear(RemoteAliasKind::Pull).await?;
            count += aliases.clear(RemoteAliasKind::Push).await?;
        } else if let Some(alias) = &self.alias {
            let both = !self.pull && !self.push;

            if (self.pull || both) && aliases.delete(alias, RemoteAliasKind::Pull).await? {
                count += 1;
            }
            if (self.push || both) && aliases.delete(alias, RemoteAliasKind::Push).await? {
                count += 1;
            }
        } else {
            return Err(CLIError::usage_error("Specify either an alias or --all"));
        }

        Ok(count)
    }

    async fn delete_all_aliases(&self) -> Result<usize, CLIError> {
        let mut count = 0;

        let mut stream = self.dataset_repo.get_all_datasets();
        while let Some(dataset_handle) =
            stream.next().await.transpose().map_err(CLIError::failure)?
        {
            let mut aliases = self
                .remote_alias_reg
                .get_remote_aliases(&dataset_handle.into_local_ref())
                .await?;

            // --all --push - clears all push aliases only
            // --all --pull - clears all pull aliases only
            // --all - clears all
            if self.pull || !self.push {
                count += aliases.clear(RemoteAliasKind::Pull).await?;
            }
            if self.push || !self.pull {
                count += aliases.clear(RemoteAliasKind::Push).await?;
            }
        }

        Ok(count)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AliasDeleteCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let count = if self.dataset.is_some() {
            self.delete_dataset_alias().await
        } else if self.all {
            self.delete_all_aliases().await
        } else {
            Err(CLIError::usage_error(
                "Need to specify either a dataset or --all",
            ))
        }?;

        eprintln!(
            "{}",
            console::style(format!("Deleted {count} alias(es)"))
                .green()
                .bold()
        );

        Ok(())
    }
}
