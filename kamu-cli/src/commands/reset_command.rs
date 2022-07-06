// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use opendatafabric::*;
use super::{CLIError, Command};
use super::common;
use kamu::domain::*;

pub struct ResetCommand {
    reset_svc: Arc<dyn ResetService>,
    dataset_ref: DatasetRefLocal,
    block_hash_as_string: Option<String>,
    no_confirmation: bool,
}

impl ResetCommand {
    pub fn new<S, R>(
        reset_svc: Arc<dyn ResetService>,
        dataset_ref: R,
        block_hash_as_string: Option<S>,
        no_confirmation: bool,
    ) -> Self 
    where
        S: Into<String>,    
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,    
    {
        Self {
            reset_svc,
            dataset_ref: dataset_ref.try_into().unwrap(),
            block_hash_as_string: block_hash_as_string.map(|s| s.into()),
            no_confirmation,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ResetCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
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

        let raw_hash = self.block_hash_as_string.as_deref().unwrap_or_default();
        let hash = Multihash::from_multibase_str(&raw_hash).unwrap();

        self
        .reset_svc
        .reset_dataset(
            &self.dataset_ref,
            &hash,
        )
        .await
        .map_err(|e| CLIError::failure(e))?;

        Ok(())
    }
}
