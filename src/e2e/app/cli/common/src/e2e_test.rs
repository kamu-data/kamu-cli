// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::future::Future;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_cli_wrapper::Kamu;
use reqwest::Url;

use crate::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////

pub async fn e2e_test<F, Fut>(kamu: Kamu, fixture_callback: F)
where
    F: FnOnce(&KamuApiServerClient) -> Fut,
    Fut: Future<Output = ()>,
{
    let host = "127.0.0.1";
    // TODO: Random port support -- this unlocks parallel running
    let port = "4000";

    let server_run_fut = async {
        kamu.execute([
            "--e2e-testing",
            "system",
            "api-server",
            "--address",
            host,
            "--http-port",
            port,
        ])
        .await
        .int_err()
    };

    let test_fut = async {
        let base_url = Url::parse(&format!("http://{host}:{port}")).unwrap();
        let kamu_api_server_client = KamuApiServerClient::new(base_url);

        kamu_api_server_client.ready().await?;

        fixture_callback(&kamu_api_server_client).await;

        kamu_api_server_client.shutdown().await?;

        Ok::<_, InternalError>(())
    };

    assert_matches!(tokio::try_join!(server_run_fut, test_fut), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////
