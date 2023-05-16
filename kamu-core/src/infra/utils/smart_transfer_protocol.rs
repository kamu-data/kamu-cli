// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::Multihash;
use url::Url;

pub use super::simple_transfer_protocol::DatasetFactoryFn;
use crate::domain::{Dataset, SyncError, SyncListener, SyncResult};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct ObjectTransferOptions {
    pub max_parallel_transfers: usize,
    pub min_upload_progress_delay_sec: u64,
}

impl Default for ObjectTransferOptions {
    fn default() -> Self {
        // Use number of allowed parallel threads on the target system.
        // Run single-threaded transfer as a fallback, if the parallelism grade cannot
        // be determined
        let max_parallel_transfers = std::thread::available_parallelism()
            .map(|nz| nz.get())
            .unwrap_or(1_usize);

        Self {
            max_parallel_transfers,
            min_upload_progress_delay_sec: 1,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SmartTransferProtocolClient: Sync + Send {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: Option<Arc<dyn Dataset>>,
        dst_factory: Option<DatasetFactoryFn>,
        listener: Arc<dyn SyncListener>,
        transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError>;

    async fn push_protocol_client_flow(
        &self,
        src: Arc<dyn Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&Multihash>,
        listener: Arc<dyn SyncListener>,
        transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
