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
use crate::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(dead_code)]
#[dill::component]
#[dill::interface(dyn Command)]
pub struct ApplyCommand {
    resource_loader: Arc<dyn ResourceLoader>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    create_dataset_from_snapshot: Arc<dyn kamu_datasets::CreateDatasetFromSnapshotUseCase>,
    delete_dataset: Arc<dyn kamu_datasets::DeleteDatasetUseCase>,
    output_config: Arc<OutputConfig>,
    tenancy_config: TenancyConfig,

    #[dill::component(explicit)]
    snapshot_refs: Vec<String>,

    #[dill::component(explicit)]
    dry_run: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    stdin: bool,

    #[dill::component(explicit)]
    dataset_visibility: odf::DatasetVisibility,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ApplyCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        unimplemented!()
    }

    async fn run(&self) -> Result<(), CLIError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
