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

use super::{CLIError, Command};

pub struct RenameCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_ref: DatasetRef,
    new_name: DatasetName,
}

impl RenameCommand {
    pub fn new<N>(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_ref: DatasetRef,
        new_name: N,
    ) -> Self
    where
        N: TryInto<DatasetName>,
        <N as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            dataset_repo,
            dataset_ref,
            new_name: new_name.try_into().unwrap(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for RenameCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match self
            .dataset_repo
            .rename_dataset(&self.dataset_ref, &self.new_name)
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
            console::style(format!("Dataset renamed")).green().bold()
        );

        Ok(())
    }
}
