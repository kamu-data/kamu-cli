// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::common;
use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use std::sync::Arc;

pub struct DeleteCommand {
    dataset_reg: Arc<dyn DatasetRegistry>,
    sync_svc: Arc<dyn SyncService>,
    dataset_refs: Vec<DatasetRefAny>,
    all: bool,
    recursive: bool,
    no_confirmation: bool,
}

impl DeleteCommand {
    pub fn new<I, R>(
        dataset_reg: Arc<dyn DatasetRegistry>,
        sync_svc: Arc<dyn SyncService>,
        dataset_refs: I,
        all: bool,
        recursive: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: IntoIterator<Item = R>,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
    {
        Self {
            dataset_reg,
            sync_svc,
            dataset_refs: dataset_refs
                .into_iter()
                .map(|s| s.try_into().unwrap())
                .collect(),
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

        let confirmed = if self.no_confirmation {
            true
        } else {
            common::prompt_yes_no(&format!(
                "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to delete following dataset(s)").yellow(),
                dataset_refs
                    .iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for r in dataset_refs.iter() {
            match r.as_local_ref() {
                Some(local_ref) => self.dataset_reg.delete_dataset(&local_ref)?,
                None => match r.as_remote_ref() {
                    Some(DatasetRefRemote::RemoteName(name)) => self
                        .sync_svc
                        .delete(&name).await
                        .map_err(|e| CLIError::failure(e))?,
                    _ => unimplemented!(),
                },
            }
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
