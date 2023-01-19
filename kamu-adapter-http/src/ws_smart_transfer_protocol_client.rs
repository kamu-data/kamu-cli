// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use url::Url;
use dill::component;
use futures::SinkExt;

use tokio_stream::StreamExt;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message},
};

use kamu::infra::utils::smart_transfer_protocol::SmartTransferProtocolClient;
use kamu::domain::{Dataset, SyncError, SyncResult, SyncListener};

/////////////////////////////////////////////////////////////////////////////////////////


#[component(pub)]
pub struct WsSmartTransferProtocolClient {}

#[async_trait::async_trait]
impl SmartTransferProtocolClient for WsSmartTransferProtocolClient {

    async fn pull_protocol_client_flow(
        &self,
        src_url: &Url,
        dst: &dyn Dataset,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {

        listener.begin();

        let mut pull_url = src_url.join("pull").unwrap();
        let pull_url_res = pull_url.set_scheme("ws");
        assert!(pull_url_res.is_ok());
    
    
        let (mut ws_stream, _) = connect_async(pull_url).await.unwrap();
    
        let hello_message = Message::Text(String::from("Hello pull server!"));
        if ws_stream.send(hello_message).await.is_err() {
            // server disconnected
        }
        while let Some(msg) = ws_stream.next().await {
            let msg = msg.unwrap();
            if msg.is_text() || msg.is_binary() {
                println!("Pull server sent: {}", msg);
                ws_stream.close(None).await.unwrap();
            }
        }
    
        unimplemented!("Not supported yet")

    }

    async fn push_protocol_client_flow(
        &self,
        src: &dyn Dataset,
        dst_url: &Url,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let mut push_url = dst_url.join("push").unwrap();
        let push_url_res = push_url.set_scheme("ws");
        assert!(push_url_res.is_ok());
    
    
        let (mut ws_stream, _) = connect_async(push_url).await.unwrap();
    
        let hello_message = Message::Text(String::from("Hello push server!"));
        if ws_stream.send(hello_message).await.is_err() {
            // server disconnected
        }
        while let Some(msg) = ws_stream.next().await {
            let msg = msg.unwrap();
            if msg.is_text() || msg.is_binary() {
                println!("Push server sent: {}", msg);
                ws_stream.close(None).await.unwrap();
            }
        }
    
        unimplemented!("Not supported yet")    
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
