// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    ServerRunFut: Future<Output = Result<String, InternalError>> + Send + 'static,
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut + Send + 'static,
    FixtureFut: Future<Output = ()> + Send + 'static,
{
    let wrapped_server_run_fut = async move {
        dbg!("@@@ wrapped_server_run_fut 1");

        let a = server_run_fut.await;

        dbg!("@@@ wrapped_server_run_fut 2");

        a
    };

    let test_fut = async move {
        dbg!("@@@ test_fut 1");

        let base_url = get_server_api_base_url(e2e_data_file_path).await?;

        dbg!("@@@ test_fut 2");

        let kamu_api_server_client = KamuApiServerClient::new(base_url);

        dbg!("@@@ test_fut 3");

        kamu_api_server_client.ready().await?;

        dbg!("@@@ test_fut 4");

        // tokio::spawn() is used to catch panic, otherwise the test will hang
        let a = tokio::spawn(fixture(kamu_api_server_client.clone())).await;

        dbg!("@@@ test_fut 5");

        kamu_api_server_client.shutdown().await?;

        dbg!("@@@ test_fut 6");

        a.int_err()
    };

    let (server_res, test_res) = tokio::join!(wrapped_server_run_fut, test_fut);

    if let Err(e) = test_res {
        let mut panic_message = format!("Fixture execution error:\n{e:?}\n");

        match server_res {
            Ok(s) => {
                panic_message += "Server output:\n";
                panic_message += s.as_str();
            }
            Err(e) => {
                panic_message += "Server execution error:\n";
                panic_message += format!("{e}").as_str();
            }
        }

        panic!("{panic_message}");
    }
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
