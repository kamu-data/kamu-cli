use super::common;
use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use std::sync::Arc;

pub struct DeleteCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    sync_svc: Arc<dyn SyncService>,
    ids: Vec<DatasetRefBuf>,
    all: bool,
    recursive: bool,
    no_confirmation: bool,
}

impl DeleteCommand {
    pub fn new<I, ID>(
        metadata_repo: Arc<dyn MetadataRepository>,
        sync_svc: Arc<dyn SyncService>,
        ids: I,
        all: bool,
        recursive: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: IntoIterator<Item = ID>,
        ID: Into<DatasetRefBuf>,
    {
        Self {
            metadata_repo,
            sync_svc,
            ids: ids.into_iter().map(|s| s.into()).collect(),
            all,
            recursive,
            no_confirmation,
        }
    }
}

impl Command for DeleteCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.ids.is_empty() && !self.all {
            return Err(CLIError::usage_error("Specify a dataset or use --all flag"));
        }

        let dataset_ids = if self.all {
            unimplemented!("Recursive deletion is not yet supported")
        } else if self.recursive {
            unimplemented!("Recursive deletion is not yet supported")
        } else {
            self.ids.clone()
        };

        let confirmed = if self.no_confirmation {
            true
        } else {
            common::prompt_yes_no(&format!(
                "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to delete following dataset(s)").yellow(),
                dataset_ids
                    .iter()
                    .map(|id| id.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for id in dataset_ids.iter() {
            match id.as_local() {
                Some(local_id) => self.metadata_repo.delete_dataset(local_id)?,
                None => self.sync_svc.delete(id).map_err(|e| CLIError::failure(e))?,
            }
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} dataset(s)", dataset_ids.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}
