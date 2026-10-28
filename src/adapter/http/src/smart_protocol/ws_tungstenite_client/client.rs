// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::utils::smart_transfer_protocol::{SmartTransferProtocolClient, TransferOptions};
use kamu_core::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SmartTransferProtocolClient)]
pub struct WsSmartTransferProtocolClient {
    pub(super) catalog: dill::Catalog,
    pub(super) dataset_credential_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SmartTransferProtocolClient for WsSmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: &LocalSyncTarget,
        transfer_options: TransferOptions,
    ) -> Result<SyncTransfer, SyncError> {
        self.pull_flow(http_src_url, dst, transfer_options).await
    }

    async fn push_protocol_client_flow(
        &self,
        src: Arc<dyn odf::Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&odf::Multihash>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        self.push_flow(src, http_dst_url, dst_head, transfer_options)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
