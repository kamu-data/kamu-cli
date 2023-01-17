// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use futures::SinkExt;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result},
};
use tokio_stream::StreamExt;

use opendatafabric::DatasetRefAny;

use crate::domain::{Dataset, AppendValidation, SyncListener, SyncResult, SyncError};

/////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Smart Transfer Protocol" as described in ODF spec
pub struct SmartTransferProtocol;

/////////////////////////////////////////////////////////////////////////////////////////

impl SmartTransferProtocol {

    pub async fn sync_smart_src<'a>(
        &'a self,
        src: &'a dyn Dataset,
        src_ref: &'a DatasetRefAny,
        dst: &'a dyn Dataset,
        _dst_ref: &'a DatasetRefAny,
        validation: AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let mut pull_url = src.base_url().join("pull").unwrap();
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


    pub async fn sync_smart_dst<'a>(
        &'a self,
        src: &'a dyn Dataset,
        src_ref: &'a DatasetRefAny,
        dst: &'a dyn Dataset,
        _dst_ref: &'a DatasetRefAny,
        validation: AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let mut push_url = src.base_url().join("push").unwrap();
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
