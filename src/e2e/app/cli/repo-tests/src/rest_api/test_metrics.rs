// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use kamu_cli_e2e_common::KamuApiServerClient;
use reqwest::{Method, StatusCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_metrics_report_agent_heartbeats(client: KamuApiServerClient) {
    // Agents record their first heartbeat when their loop starts, which may
    // follow the server becoming ready
    let mut metrics = String::new();
    for _ in 0..50 {
        let response = client
            .rest_api_call(Method::GET, "system/metrics", None)
            .await;
        pretty_assertions::assert_eq!(StatusCode::OK, response.status());

        metrics = response.text().await.unwrap();
        if metrics.contains(
            r#"wakeup_listener_last_heartbeat_timestamp_seconds{agent="dev.kamu.utils.messaging.OutboxAgent"}"#,
        ) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("No outbox agent heartbeat in metrics:\n{metrics}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
