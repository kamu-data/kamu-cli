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
use std::net::SocketAddrV4;

use internal_error::InternalError;
use reqwest::Url;

use crate::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////

pub async fn e2e_test<ServerRunFut, Fixture, FixtureFut>(
    server_addr: SocketAddrV4,
    server_run_fut: ServerRunFut,
    fixture: Fixture,
) where
    ServerRunFut: Future<Output = Result<(), InternalError>>,
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()>,
{
    let test_fut = async move {
        let base_url = Url::parse(&format!("http://{server_addr}")).unwrap();
        let kamu_api_server_client = KamuApiServerClient::new(base_url);

        kamu_api_server_client.ready().await?;
        {
            fixture(kamu_api_server_client.clone()).await;
        }
        kamu_api_server_client.shutdown().await?;

        Ok::<_, InternalError>(())
    };

    assert_matches!(tokio::try_join!(server_run_fut, test_fut), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////
