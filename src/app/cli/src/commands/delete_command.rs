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

use super::{common, CLIError, Command};

pub struct DeleteCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_refs: Vec<DatasetRef>,
    all: bool,
    recursive: bool,
    no_confirmation: bool,
}

impl DeleteCommand {
    pub fn new<I>(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_refs: I,
        all: bool,
        recursive: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: IntoIterator<Item = DatasetRef>,
    {
        Self {
            dataset_repo,
            dataset_refs: dataset_refs.into_iter().collect(),
            all,
            recursive,
            no_confirmation,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for DeleteCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if self.dataset_refs.is_empty() && !self.all {
            return Err(CLIError::usage_error("Specify a dataset or use --all flag"));
        }

        let dataset_refs = if self.all {
            unimplemented!("Recursive deletion is not yet supported")
        } else if self.recursive {
            unimplemented!("Recursive deletion is not yet supported")
        } else {
            self.dataset_refs.clone()
        };

        // Check references exist
        // TODO: PERF: Create a batch version of `resolve_dataset_ref`
        for dataset_ref in &self.dataset_refs {
            match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
                Ok(_) => Ok(()),
                Err(GetDatasetError::NotFound(e)) => Err(CLIError::usage_error_from(e)),
                Err(GetDatasetError::Internal(e)) => Err(e.into()),
            }?;
        }

        let confirmed = if self.no_confirmation {
            true
        } else {
            common::prompt_yes_no(&format!(
                "{}: {}\n{}\nDo you wish to continue? [y/N]: ",
                console::style("You are about to delete following dataset(s)").yellow(),
                dataset_refs
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for dataset_ref in &dataset_refs {
            match self.dataset_repo.delete_dataset(dataset_ref).await {
                Ok(_) => Ok(()),
                Err(DeleteDatasetError::DanglingReference(e)) => Err(CLIError::failure(e)),
                Err(DeleteDatasetError::Access(e)) => Err(CLIError::failure(e)),
                Err(e) => Err(CLIError::critical(e)),
            }?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} dataset(s)", dataset_refs.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}
