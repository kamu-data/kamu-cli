// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::TcpStream;
use url::Url;
use dill::component;
use futures::SinkExt;

use tokio_stream::StreamExt;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message}, WebSocketStream, MaybeTlsStream,
};

use kamu::{infra::utils::smart_transfer_protocol::SmartTransferProtocolClient, domain::BlockRef};
use kamu::domain::{Dataset, SyncError, SyncResult, SyncListener};

use crate::{messages::*, ws_common::{ReadMessageError, WriteMessageError, self}};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct WsSmartTransferProtocolClient {}

/////////////////////////////////////////////////////////////////////////////////////////

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

        let dst_head = dst.as_metadata_chain().get_ref(&BlockRef::Head).await.unwrap();

        let pull_request_message = DatasetPullRequest {
            begin_after: Some(dst_head),
            stop_at: None,
        };

        let write_result = 
            tungstenite_ws_write_generic_payload(&mut ws_stream, pull_request_message).await;
        if write_result.is_err() {
            panic!("write problem")
        }

        let read_result = 
            tungstenite_ws_read_generic_payload::<DatasetPullResponse>(&mut ws_stream).await;
        
        if read_result.is_err() {
            panic!("read problem")
        }            
    
        let dataset_pull_response: DatasetPullResponse = read_result.unwrap();
        println!(
            "Pull response estimate: {} blocks to synchronize", 
            dataset_pull_response.size_estimation.num_blocks
        );
    
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

type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/////////////////////////////////////////////////////////////////////////////////


pub async fn tungstenite_ws_read_generic_payload<TMessagePayload: DeserializeOwned>(
    stream: &mut TungsteniteStream,
) -> Result<TMessagePayload, ReadMessageError>{

    match stream.next().await {
        Some(msg) => {
            match msg {
                Ok(Message::Text(raw_message)) => {
                    ws_common::parse_payload::<TMessagePayload>(raw_message)
                },
                Ok(_) => Err(ReadMessageError::NonTextMessageReceived),
                Err(e) => Err(ReadMessageError::SocketError(Box::new(e))),
            }
        },
        None => Err(ReadMessageError::ClientDisconnected)
    }
}


/////////////////////////////////////////////////////////////////////////////////


pub async fn tungstenite_ws_write_generic_payload<TMessagePayload: Serialize>(
    socket: &mut TungsteniteStream,
    payload: TMessagePayload
) -> Result<(), WriteMessageError>{

    let payload_as_json_string = 
        ws_common::payload_to_json::<TMessagePayload>(payload)?;

    let message = Message::Text(payload_as_json_string);
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e)))
    }
}


/////////////////////////////////////////////////////////////////////////////////
