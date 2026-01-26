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
use kamu_cli_puppet::extensions::ServerOutput;
use reqwest::{Client, Url};
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;

use crate::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ENV_ELASTICSEARCH_URL: &str = "ELASTICSEARCH_URL";
const ENV_ELASTICSEARCH_PASSWORD: &str = "ELASTICSEARCH_PASSWORD";

const DEFAULT_ELASTICSEARCH_URL: &str = "http://localhost:9200";
const DEFAULT_ELASTICSEARCH_USERNAME: &str = "elastic";
const DEFAULT_ELASTICSEARCH_PASSWORD: &str = "root";

#[derive(Clone, Debug)]
pub struct ElasticsearchTestCleanup {
    pub index_prefix: String,
}

pub async fn api_server_e2e_test<ServerRunFut, Fixture, FixtureFut>(
    e2e_data_file_path: PathBuf,
    workspace_path: PathBuf,
    server_run_fut: ServerRunFut,
    fixture: Fixture,
    elasticsearch_cleanup: Option<ElasticsearchTestCleanup>,
) where
    ServerRunFut: Future<Output = ServerOutput>,
    Fixture: FnOnce(KamuApiServerClient) -> FixtureFut,
    FixtureFut: Future<Output = ()> + Send + 'static,
{
    let test_fut = async move {
        let base_url = get_server_api_base_url(e2e_data_file_path).await?;
        let kamu_api_server_client = KamuApiServerClient::new(base_url, workspace_path);

        kamu_api_server_client.e2e().ready().await?;

        let fixture_res = {
            // tokio::spawn() is used to catch panic, otherwise the test will hang
            tokio::spawn(fixture(kamu_api_server_client.clone())).await
        };

        kamu_api_server_client.e2e().shutdown().await?;

        if fixture_res.is_ok()
            && let Some(cleanup) = elasticsearch_cleanup
            && let Err(e) = cleanup_elasticsearch_test_data(&cleanup.index_prefix).await
        {
            eprintln!(
                "Elasticsearch cleanup failed for prefix '{}': {e:?}",
                cleanup.index_prefix
            );
        }

        fixture_res.int_err()
    };

    let (server_output, test_res) = tokio::join!(server_run_fut, test_fut);

    if let Err(e) = test_res {
        let mut panic_message = format!("Fixture execution error:\n{e:?}\n");

        panic_message += "Server output:\n";
        panic_message += "stdout:\n";
        panic_message += server_output.stdout.as_str();
        panic_message += "\n";
        panic_message += "stderr:\n";
        panic_message += server_output.stderr.as_str();
        panic_message += "\n";

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

async fn cleanup_elasticsearch_test_data(index_prefix: &str) -> Result<(), InternalError> {
    let es_url = std::env::var(ENV_ELASTICSEARCH_URL)
        .unwrap_or_else(|_| DEFAULT_ELASTICSEARCH_URL.to_string());
    let es_url = Url::parse(&es_url).int_err()?;

    if es_url.scheme() != "http" {
        return Err(InternalError::new(format!(
            "Elasticsearch cleanup supports http only, got: {}",
            es_url.scheme()
        )));
    }

    let es_password = std::env::var(ENV_ELASTICSEARCH_PASSWORD)
        .ok()
        .unwrap_or_else(|| DEFAULT_ELASTICSEARCH_PASSWORD.to_string());

    let client = Client::new();

    let aliases_url = es_url.join("_cat/aliases?h=alias,index").int_err()?;
    let aliases_resp = client
        .get(aliases_url)
        .basic_auth(DEFAULT_ELASTICSEARCH_USERNAME, Some(&es_password))
        .send()
        .await
        .int_err()?
        .error_for_status()
        .int_err()?
        .text()
        .await
        .int_err()?;

    let aliases: Vec<String> = aliases_resp
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|alias| alias.starts_with(index_prefix))
        .map(str::to_string)
        .collect();

    for alias in aliases {
        let body = serde_json::json!({
            "actions": [{
                "remove": {
                    "index": "*",
                    "alias": alias,
                }
            }]
        });
        let aliases_update_url = es_url.join("_aliases").int_err()?;
        let _ = client
            .post(aliases_update_url)
            .basic_auth(DEFAULT_ELASTICSEARCH_USERNAME, Some(&es_password))
            .json(&body)
            .send()
            .await;
    }

    let indices_url = es_url
        .join(&format!("_cat/indices/{index_prefix}*?h=index"))
        .int_err()?;
    let indices_resp = client
        .get(indices_url)
        .basic_auth(DEFAULT_ELASTICSEARCH_USERNAME, Some(&es_password))
        .send()
        .await
        .int_err()?
        .error_for_status()
        .int_err()?
        .text()
        .await
        .int_err()?;

    for index in indices_resp
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let delete_url = es_url.join(index).int_err()?;
        let _ = client
            .delete(delete_url)
            .basic_auth(DEFAULT_ELASTICSEARCH_USERNAME, Some(&es_password))
            .query(&[("ignore_unavailable", "true")])
            .send()
            .await;
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
