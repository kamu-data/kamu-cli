// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::{SyncError, SyncListener, SyncResult};
use url::Url;

use crate::utils::smart_transfer_protocol::{SmartTransferProtocolClient, TransferOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SmartTransferProtocolClient)]
pub struct DummySmartTransferProtocolClient {}

#[async_trait::async_trait]
impl SmartTransferProtocolClient for DummySmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        _http_src_url: &Url,
        _dst: Option<Arc<dyn odf::Dataset>>,
        _dst_alias: Option<&odf::DatasetAlias>,
        _dst_handle: Option<&odf::DatasetHandle>,
        _listener: Arc<dyn SyncListener>,
        _transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!("Not supported yet")
    }

    async fn push_protocol_client_flow(
        &self,
        _src: Arc<dyn odf::Dataset>,
        _http_dst_url: &Url,
        _dst_head: Option<&odf::Multihash>,
        _listener: Arc<dyn SyncListener>,
        _transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!("Not supported yet")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
