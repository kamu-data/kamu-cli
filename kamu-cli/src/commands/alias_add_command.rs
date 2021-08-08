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
            return Err(CLIError::UsageError {
                msg: "Specify either --pull or --push or both".to_owned(),
            });
        }

        let dataset_id = DatasetID::try_from(&self.dataset).unwrap();
        let remote_ref = DatasetRefBuf::try_from(self.alias.clone()).unwrap();

        let repo_id = remote_ref.repository().ok_or(CLIError::UsageError {
            msg: "Alias should contain a repository part".to_owned(),
        })?;

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
