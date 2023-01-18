// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::{
    infra::utils::smart_transfer_protocol::SmartTransferProtocolClient, 
    domain::{Dataset, SyncListener, SyncResult, SyncError}
};


pub struct DummySmartTransferProtocolClient {}

impl DummySmartTransferProtocolClient {
    pub fn new() -> DummySmartTransferProtocolClient{
        Self {}
    }
}

#[async_trait::async_trait]
impl SmartTransferProtocolClient for DummySmartTransferProtocolClient {

    async fn pull_protocol_client_flow(
        &self,
        src: & dyn Dataset,
        dst: & dyn Dataset,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {

        unimplemented!("Not supported yet")

    }

    async fn push_protocol_client_flow(
        &self,
        src: & dyn Dataset,
        dst: & dyn Dataset,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {

        unimplemented!("Not supported yet")
 
    }
}
