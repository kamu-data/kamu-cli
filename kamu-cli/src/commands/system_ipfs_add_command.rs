// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct SystemIpfsAddCommand {
    sync_svc: Arc<dyn SyncService>,
    dataset_ref: DatasetRefLocal,
}

impl SystemIpfsAddCommand {
    pub fn new<R>(sync_svc: Arc<dyn SyncService>, dataset_ref: R) -> Self
    where
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,
    {
        Self {
            sync_svc,
            dataset_ref: dataset_ref.try_into().unwrap(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemIpfsAddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let cid = self
            .sync_svc
            .ipfs_add(&self.dataset_ref)
            .await
            .map_err(|e| CLIError::failure(e))?;

        println!("{}", cid);

        Ok(())
    }
}
