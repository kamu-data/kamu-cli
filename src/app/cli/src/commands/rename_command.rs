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

#[dill::component]
#[dill::interface(dyn Command)]
pub struct RenameCommand {
    rename_dataset: Arc<dyn RenameDatasetUseCase>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    new_name: odf::DatasetName,
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
