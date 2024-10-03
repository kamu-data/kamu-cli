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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemIpfsAddCommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    sync_svc: Arc<dyn SyncService>,
    dataset_ref: DatasetRef,
}

impl SystemIpfsAddCommand {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        sync_svc: Arc<dyn SyncService>,
        dataset_ref: DatasetRef,
    ) -> Self {
        Self {
            dataset_registry,
            sync_svc,
            dataset_ref,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemIpfsAddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let dataset = self
            .dataset_registry
            .get_dataset_by_ref(&self.dataset_ref)
            .await
            .map_err(CLIError::failure)?;

        let cid = self
            .sync_svc
            .ipfs_add(dataset)
            .await
            .map_err(CLIError::failure)?;

        println!("{cid}");

        Ok(())
    }
}
