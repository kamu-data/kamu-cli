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

pub struct AliasDeleteCommand {
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset: DatasetRefLocal,
    alias: Option<RemoteDatasetName>,
    all: bool,
    pull: bool,
    push: bool,
}

impl AliasDeleteCommand {
    pub fn new<R, N>(
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset: R,
        alias: Option<N>,
        all: bool,
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
            remote_alias_reg,
            dataset: dataset.try_into().unwrap(),
            alias: alias.map(|s| s.try_into().unwrap()),
            all,
            pull,
            push,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AliasDeleteCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let mut aliases = self.remote_alias_reg.get_remote_aliases(&self.dataset)?;

        let mut count = 0;

        if self.all {
            count += aliases.clear(RemoteAliasKind::Pull)?;
            count += aliases.clear(RemoteAliasKind::Push)?;
        } else if let Some(alias) = &self.alias {
            let both = !self.pull && !self.push;

            if self.pull || both {
                if aliases.delete(alias, RemoteAliasKind::Pull)? {
                    count += 1;
                }
            }
            if self.push || both {
                if aliases.delete(alias, RemoteAliasKind::Push)? {
                    count += 1;
                }
            }
        } else {
            return Err(CLIError::usage_error("Specify either an alias or --all"));
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} alias(es)", count))
                .green()
                .bold()
        );

        Ok(())
    }
}
