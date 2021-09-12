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
use opendatafabric::{DatasetID, DatasetRef};

use std::sync::Arc;

pub struct AliasDeleteCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset: String,
    alias: Option<String>,
    all: bool,
    pull: bool,
    push: bool,
}

impl AliasDeleteCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset: String,
        alias: Option<String>,
        all: bool,
        pull: bool,
        push: bool,
    ) -> Self {
        Self {
            metadata_repo,
            dataset,
            alias,
            all,
            pull,
            push,
        }
    }
}

impl Command for AliasDeleteCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let dataset_id = DatasetID::try_from(&self.dataset).unwrap();
        let mut aliases = self.metadata_repo.get_remote_aliases(dataset_id)?;

        let mut count = 0;

        if self.all {
            count += aliases.clear(RemoteAliasKind::Pull)?;
            count += aliases.clear(RemoteAliasKind::Push)?;
        } else if let Some(ref alias) = self.alias {
            let remote_ref = DatasetRef::try_from(&alias).unwrap();
            let both = !self.pull && !self.push;

            if self.pull || both {
                if aliases.delete(remote_ref, RemoteAliasKind::Pull)? {
                    count += 1;
                }
            }
            if self.push || both {
                if aliases.delete(remote_ref, RemoteAliasKind::Push)? {
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
