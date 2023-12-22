// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::{Dataset, SyncError, SyncListener, SyncResult};
use opendatafabric::Multihash;
use url::Url;

#[dill::component]
#[dill::interface(dyn SmartTransferProtocolClient)]
use crate::utils::simple_transfer_protocol::DatasetFactoryFn;
use crate::utils::smart_transfer_protocol::{ObjectTransferOptions, SmartTransferProtocolClient};

pub struct DummySmartTransferProtocolClient {}

impl DummySmartTransferProtocolClient {
    pub fn new() -> DummySmartTransferProtocolClient {
        Self {}
    }
}

#[async_trait::async_trait]
impl SmartTransferProtocolClient for DummySmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        _http_src_url: &Url,
        _dst: Option<Arc<dyn Dataset>>,
        _dst_factory: Option<DatasetFactoryFn>,
        _listener: Arc<dyn SyncListener>,
        _transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!("Not supported yet")
    }

    async fn push_protocol_client_flow(
        &self,
        _src: Arc<dyn Dataset>,
        _http_dst_url: &Url,
        _dst_head: Option<&Multihash>,
        _listener: Arc<dyn SyncListener>,
        _transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!("Not supported yet")
    }
}
