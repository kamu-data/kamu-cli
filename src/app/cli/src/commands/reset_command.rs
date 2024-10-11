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
use crate::Interact;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResetCommand {
    interact: Arc<Interact>,
    dataset_repo: Arc<dyn DatasetRepository>,
    reset_svc: Arc<dyn ResetService>,
    dataset_ref: DatasetRef,
    block_hash: Multihash,
}

impl ResetCommand {
    pub fn new(
        interact: Arc<Interact>,
        dataset_repo: Arc<dyn DatasetRepository>,
        reset_svc: Arc<dyn ResetService>,
        dataset_ref: DatasetRef,
        block_hash: Multihash,
    ) -> Self {
        Self {
            interact,
            dataset_repo,
            reset_svc,
            dataset_ref,
            block_hash,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ResetCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await?;

        self.interact.require_confirmation(format!(
            "{}: {}\n{}",
            console::style("You are about to reset the following dataset").yellow(),
            self.dataset_ref,
            console::style("This operation is irreversible!").yellow(),
        ))?;

        self.reset_svc
            .reset_dataset(&dataset_handle, Some(&self.block_hash), None)
            .await
            .map_err(CLIError::failure)?;

        eprintln!("{}", console::style("Dataset was reset").green().bold());

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
