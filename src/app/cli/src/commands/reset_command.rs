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

use super::{CLIError, Command};
use crate::Interact;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ResetCommand {
    interact: Arc<Interact>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    reset_dataset_use_case: Arc<dyn ResetDatasetUseCase>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    block_hash: odf::Multihash,
}

#[async_trait::async_trait(?Send)]
impl Command for ResetCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let dataset_handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&self.dataset_ref)
            .await?;

        self.interact.require_confirmation(format!(
            "{}: {}\n{}",
            console::style("You are about to reset the following dataset").yellow(),
            self.dataset_ref,
            console::style("This operation is irreversible!").yellow(),
        ))?;

        self.reset_dataset_use_case
            .execute(&dataset_handle, Some(&self.block_hash), None)
            .await
            .map_err(CLIError::failure)?;

        eprintln!("{}", console::style("Dataset was reset").green().bold());

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
