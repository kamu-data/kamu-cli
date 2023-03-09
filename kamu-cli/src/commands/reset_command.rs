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

pub struct ResetCommand {
    local_repo: Arc<dyn DatasetRepository>,
    reset_svc: Arc<dyn ResetService>,
    dataset_ref: DatasetRefLocal,
    block_hash: Multihash,
    no_confirmation: bool,
}

impl ResetCommand {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        reset_svc: Arc<dyn ResetService>,
        dataset_ref: DatasetRefLocal,
        block_hash: Multihash,
        no_confirmation: bool,
    ) -> Self {
        Self {
            local_repo,
            reset_svc,
            dataset_ref,
            block_hash,
            no_confirmation,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ResetCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let dataset_handle = self
            .local_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await?;

        let confirmed = if self.no_confirmation {
            true
        } else {
            common::prompt_yes_no(&format!(
                "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to reset the following dataset").yellow(),
                self.dataset_ref.to_string(),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        self.reset_svc
            .reset_dataset(&dataset_handle, &self.block_hash)
            .await
            .map_err(|e| CLIError::failure(e))?;

        Ok(())
    }
}
