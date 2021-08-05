use super::{Command, Error};
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
    fn run(&mut self) -> Result<(), Error> {
        if !self.pull && !self.push {
            return Err(Error::UsageError {
                msg: "Specify either --pull or --push or both".to_owned(),
            });
        }

        let dataset_id = DatasetID::try_from(&self.dataset).unwrap();
        let remote_ref = DatasetRefBuf::try_from(self.alias.clone()).unwrap();

        let remote_id = remote_ref.remote_id().ok_or(Error::UsageError {
            msg: "Alias should contain a remote part".to_owned(),
        })?;

        self.metadata_repo.get_remote(remote_id)?;
        let mut aliases = self.metadata_repo.get_remote_aliases(dataset_id)?;

        let mut count = 0;

        if self.pull {
            if aliases.add(remote_ref.clone(), RemoteAliasKind::Pull)? {
                count += 1;
            }
        }

        if self.push {
            if aliases.add(remote_ref.clone(), RemoteAliasKind::Push)? {
                count += 1;
            }
        }

        eprintln!(
            "{}",
            console::style(format!("Added {} alias(es)", count))
                .green()
                .bold()
        );

        Ok(())
    }
}
