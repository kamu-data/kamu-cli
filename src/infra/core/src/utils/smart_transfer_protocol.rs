// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::{ResolvedDataset, SyncError, SyncListener, SyncResult};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct TransferOptions {
    pub max_parallel_transfers: usize,
    pub force_update_if_diverged: bool,
    pub visibility_for_created_dataset: odf::DatasetVisibility,
}

impl Default for TransferOptions {
    fn default() -> Self {
        // Use number of allowed parallel threads on the target system.
        // Run single-threaded transfer as a fallback, if the parallelism grade cannot
        // be determined
        let max_parallel_transfers = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1_usize);

        Self {
            max_parallel_transfers,
            force_update_if_diverged: false,
            visibility_for_created_dataset: odf::DatasetVisibility::Private,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SmartTransferProtocolClient: Sync + Send {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: Option<&ResolvedDataset>,
        dst_alias: Option<&odf::DatasetAlias>,
        listener: Arc<dyn SyncListener>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError>;

    async fn push_protocol_client_flow(
        &self,
        src: Arc<dyn odf::Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&odf::Multihash>,
        listener: Arc<dyn SyncListener>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
