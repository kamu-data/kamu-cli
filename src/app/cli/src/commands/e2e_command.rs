// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu::domain::{DatasetRepository, MetadataChainExt};
use opendatafabric::DatasetRef;

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct E2ECommand {
    action: String,
    dataset_ref: Option<DatasetRef>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl E2ECommand {
    pub fn new<S>(
        action: S,
        dataset_ref: Option<DatasetRef>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            action: action.into(),
            dataset_ref,
            dataset_repo,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for E2ECommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match self.action.as_str() {
            "get-last-data-block-path" => {
                let Some(dataset_ref) = self.dataset_ref.as_ref() else {
                    return Err(CLIError::usage_error("dataset required"));
                };

                let dataset = self.dataset_repo.get_dataset(dataset_ref).await?;

                let maybe_physical_hash = dataset
                    .as_metadata_chain()
                    .last_data_block_with_new_data()
                    .await?
                    .into_event()
                    .and_then(|event| event.new_data)
                    .map(|new_data| new_data.physical_hash);

                let Some(physical_hash) = maybe_physical_hash else {
                    return Err(CLIError::usage_error("DataSlice not found"));
                };

                let internal_url = dataset
                    .as_data_repo()
                    .get_internal_url(&physical_hash)
                    .await;

                let path =
                    kamu_data_utils::data::local_url::into_local_path(internal_url).int_err()?;

                println!("{}", path.display());
            }
            unexpected_action => panic!("Unexpected action: '{unexpected_action}'"),
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
