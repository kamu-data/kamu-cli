// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{sync::Arc};
use opendatafabric::{Multihash};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::{net::TcpStream};
use url::Url;
use dill::component;
use futures::SinkExt;


use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message}, WebSocketStream, MaybeTlsStream,
};

use kamu::{infra::utils::smart_transfer_protocol::SmartTransferProtocolClient, domain::BlockRef};
use kamu::domain::{Dataset, SyncError, SyncResult, SyncListener};

use crate::{
    messages::*, 
    ws_common::{ReadMessageError, WriteMessageError, self}, 
    dataset_protocol_helper::{dataset_import_pulled_metadata, dataset_import_object_file}
};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct WsSmartTransferProtocolClient {}

/////////////////////////////////////////////////////////////////////////////////////////


#[derive(Error, Debug)]
enum SmartProtocolPullClientError {
    #[error(transparent)]
    PullRequestWriteFailed(WriteMessageError),
    
    #[error(transparent)]
    PullResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PullMetadataRequestWriteFailed(WriteMessageError),
        
    #[error(transparent)]
    PullMetadataResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PullObjectRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullObjectResponseReadFailed(ReadMessageError)
}


/////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {

    async fn pull_send_request(
        &self,
        socket: &mut TungsteniteStream,
        dst_head: Multihash
    ) -> Result<DatasetPullResponse, SmartProtocolPullClientError> {

        let pull_request_message = DatasetPullRequest {
            begin_after: Some(dst_head),
            stop_at: None,
        };

        let write_result = 
            tungstenite_ws_write_generic_payload(socket, pull_request_message).await;
        if let Err(e) = write_result {
            return Err(SmartProtocolPullClientError::PullRequestWriteFailed(e));
        }

        let read_result = 
            tungstenite_ws_read_generic_payload::<DatasetPullResponse>(socket).await;
        
        if let Err(e) = read_result {
            return Err(SmartProtocolPullClientError::PullResponseReadFailed(e));
        }           

        let dataset_pull_response = read_result.unwrap();

        println!(
            "Pull response estimate: {} blocks to synchronize of {} total bytes, {} data objects of {} total bytes", 
            dataset_pull_response.size_estimation.num_blocks,
            dataset_pull_response.size_estimation.bytes_in_raw_blocks,
            dataset_pull_response.size_estimation.num_objects,
            dataset_pull_response.size_estimation.bytes_in_raw_objects,
        );
    
        Ok(dataset_pull_response)
    }

    async fn pull_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<DatasetMetadataPullResponse, SmartProtocolPullClientError> {

        let pull_metadata_request = DatasetPullMetadataRequest {};
        let write_result_metadata = 
            tungstenite_ws_write_generic_payload(socket, pull_metadata_request).await;
        if let Err(e) = write_result_metadata {
            return Err(SmartProtocolPullClientError::PullMetadataRequestWriteFailed(e));
        }

        let read_result_metadata = 
            tungstenite_ws_read_generic_payload::<DatasetMetadataPullResponse>(socket).await;
        
        if let Err(e) = read_result_metadata {
            return Err(SmartProtocolPullClientError::PullMetadataResponseReadFailed(e));
        }            
    
        let dataset_metadata_pull_response: DatasetMetadataPullResponse = read_result_metadata.unwrap();
        println!(
            "Obtained object batch with {} objects of type {:?}, media type {:?}, encoding {:?}, bytes in compressed blocks {}",
            dataset_metadata_pull_response.blocks.objects_count,
            dataset_metadata_pull_response.blocks.object_type,
            dataset_metadata_pull_response.blocks.media_type,
            dataset_metadata_pull_response.blocks.encoding,
            dataset_metadata_pull_response.blocks.payload.len()
        );

        Ok(dataset_metadata_pull_response)

    }

    async fn pull_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>
    ) -> Result<DatasetPullObjectsTransferResponse, SmartProtocolPullClientError> {

        let pull_objects_request = DatasetPullObjectsTransferRequest { object_files };
        let write_result_objects = 
            tungstenite_ws_write_generic_payload(socket, pull_objects_request).await;
        if let Err(e) = write_result_objects {
            return Err(SmartProtocolPullClientError::PullObjectRequestWriteFailed(e));
        }

        let read_result_objects = 
            tungstenite_ws_read_generic_payload::<DatasetPullObjectsTransferResponse>(socket).await;
        
        if let Err(e) = read_result_objects {
            return Err(SmartProtocolPullClientError::PullObjectResponseReadFailed(e));
        }

        let dataset_objects_pull_response = read_result_objects.unwrap();
        
        dataset_objects_pull_response.object_transfer_strategies.iter()
            .for_each(
                |s| {
                    println!(
                        "Object file {} needs to download from {} via {:?} method, expires at {:?}",
                        s.object_file.physical_hash.to_string(),
                        s.download_from.url,
                        s.pull_strategy,
                        s.download_from.expires_at
                    )
                }
            );

        Ok(dataset_objects_pull_response)
    }

}

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

        let dataset_pull_response = 
            self.pull_send_request(&mut ws_stream, dst_head.clone())
                .await
                .unwrap();

        let sync_result = if dataset_pull_response.size_estimation.num_blocks > 0 {

            let dataset_pull_metadata_response =
            self.pull_send_metadata_request(&mut ws_stream)
                .await
                .unwrap();

            let object_files_transfer_plan = 
                dataset_import_pulled_metadata(dst, dataset_pull_metadata_response.blocks)
                    .await;

            println!("Object files transfer plan consist of {} stages", object_files_transfer_plan.len());

            let mut stage_index = 0;
            for stage_object_files in object_files_transfer_plan {
                stage_index += 1;
                println!("Stage #{}: querying {} data objects", stage_index, stage_object_files.len());

                let dataset_objects_pull_response =
                self.pull_send_objects_request(&mut ws_stream, stage_object_files)
                    .await
                    .unwrap();

                use futures::StreamExt;
                futures::stream::iter(dataset_objects_pull_response.object_transfer_strategies)
                    .for_each_concurrent(
                        /* limit */ 4, // TODO: external configuration?
                        |s| async move {
                            dataset_import_object_file(dst, &s).await.unwrap()
                        }
                    ).await;
            }

            let new_dst_head = dst.as_metadata_chain().get_ref(&BlockRef::Head).await.unwrap();

            SyncResult::Updated {
                old_head: Some(dst_head),
                new_head: new_dst_head,
                num_blocks: dataset_pull_response.size_estimation.num_blocks as usize,
            }
        } else {
            SyncResult::UpToDate
        };

        ws_stream.close(None).await.unwrap();

        Ok(sync_result)

    }

    async fn push_protocol_client_flow(
        &self,
        _src: &dyn Dataset,
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

        use tokio_stream::StreamExt;
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

    use tokio_stream::StreamExt;
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
