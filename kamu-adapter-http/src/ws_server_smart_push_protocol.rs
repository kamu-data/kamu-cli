// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::Dataset;


pub async fn dataset_push_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset: Arc<dyn Dataset>
) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };
        println!("Push client sent: {}", msg.to_text().unwrap());

        let reply = axum::extract::ws::Message::Text(String::from("Hi push client!"));
        if socket.send(reply).await.is_err() {
            // client disconnected
            return;
        }
    }        
}
