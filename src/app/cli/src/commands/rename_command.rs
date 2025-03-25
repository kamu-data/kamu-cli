// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_datasets::{RenameDatasetError, RenameDatasetUseCase};

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameCommand {
    rename_dataset: Arc<dyn RenameDatasetUseCase>,
    dataset_ref: odf::DatasetRef,
    new_name: odf::DatasetName,
}

impl RenameCommand {
    pub fn new<N>(
        rename_dataset: Arc<dyn RenameDatasetUseCase>,
        dataset_ref: odf::DatasetRef,
        new_name: N,
    ) -> Self
    where
        N: TryInto<odf::DatasetName>,
        <N as TryInto<odf::DatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            rename_dataset,
            dataset_ref,
            new_name: new_name.try_into().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for RenameCommand {
    async fn run(&self) -> Result<(), CLIError> {
        match self
            .rename_dataset
            .execute(&self.dataset_ref, &self.new_name)
            .await
        {
            Ok(_) => Ok(()),
            Err(RenameDatasetError::NotFound(e)) => Err(CLIError::failure(e)),
            Err(RenameDatasetError::NameCollision(e)) => Err(CLIError::failure(e)),
            Err(RenameDatasetError::Access(e)) => Err(CLIError::failure(e)),
            Err(e) => Err(CLIError::critical(e)),
        }?;

        eprintln!(
            "{}",
            console::style("Dataset renamed".to_string()).green().bold()
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
