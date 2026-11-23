// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use kamu::domain::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SystemIpfsAddCommand {
    catalog: dill::Catalog,
    ipfs_service: Arc<dyn IpfsService>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,
}

#[async_trait::async_trait(?Send)]
impl Command for SystemIpfsAddCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_dataset = transaction!(
            |dataset_registry: Arc<dyn DatasetRegistry>| -> Result<_, CLIError> {
                let resolved_dataset = dataset_registry
                    .get_dataset_by_ref(&self.dataset_ref)
                    .await
                    .map_err(CLIError::failure)?;
                resolved_dataset.detach_from_transaction();
                Ok(resolved_dataset)
            }
        )?;

        let cid = self
            .ipfs_service
            .add_dataset(resolved_dataset)
            .await
            .map_err(CLIError::failure)?;

        println!("{cid}");

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
