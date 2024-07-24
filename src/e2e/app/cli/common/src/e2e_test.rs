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
use std::path::PathBuf;

use internal_error::{InternalError, ResultIntoInternal};
use reqwest::Url;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

use crate::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn api_server_e2e_test<ServerRunFut, Fixture, FixtureFut>(
    e2e_data_file_path: PathBuf,
    server_run_fut: ServerRunFut,
    fixture: Fixture,
) where
    ServerRunFut: Future<Output = ()>,
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()>,
{
    let wrapped_server_run_fut = async move {
        server_run_fut.await;

        Ok::<_, InternalError>(())
    };
    let test_fut = async move {
        let base_url = get_server_api_base_url(e2e_data_file_path).await?;
        let kamu_api_server_client = KamuApiServerClient::new(base_url);

        kamu_api_server_client.ready().await?;
        {
            fixture(kamu_api_server_client.clone()).await;
        }
        kamu_api_server_client.shutdown().await?;

        Ok::<_, InternalError>(())
    };

    assert_matches!(tokio::try_join!(wrapped_server_run_fut, test_fut), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_server_api_base_url(e2e_data_file_path: PathBuf) -> Result<Url, InternalError> {
    let retry_strategy = FixedInterval::from_millis(500).take(10);
    let base_url = Retry::spawn(retry_strategy, || async {
        let data = tokio::fs::read_to_string(e2e_data_file_path.clone())
            .await
            .int_err()?;

        Url::parse(data.as_str()).int_err()
    })
    .await?;

    Ok(base_url)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
