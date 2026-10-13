// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reads the ID of a remote dataset from its `/metadata` endpoint.
///
/// Best-effort: returns `None` when the server lacks the endpoint, is
/// unreachable, or reports no seed.
pub(crate) async fn try_fetch_remote_dataset_id(
    http_dataset_url: &Url,
    maybe_access_token: Option<&str>,
) -> Option<odf::DatasetID> {
    match fetch_remote_dataset_id(http_dataset_url, maybe_access_token).await {
        Ok(maybe_dataset_id) => maybe_dataset_id,
        Err(e) => {
            tracing::debug!(%http_dataset_url, error = ?e, "Failed to fetch remote dataset ID");
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn fetch_remote_dataset_id(
    http_dataset_url: &Url,
    maybe_access_token: Option<&str>,
) -> Result<Option<odf::DatasetID>, InternalError> {
    let mut metadata_url = http_dataset_url.join("metadata").int_err()?;
    metadata_url.set_query(Some("include=Seed"));

    let mut request = reqwest::Client::new().get(metadata_url);
    if let Some(access_token) = maybe_access_token {
        request = request.bearer_auth(access_token);
    }

    let response = request
        .send()
        .await
        .int_err()?
        .error_for_status()
        .int_err()?
        .json::<MetadataSeedResponse>()
        .await
        .int_err()?;

    Ok(response.output.seed.map(|seed| seed.dataset_id))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Only the part of the `/metadata` response that is needed here. Unknown fields
// are ignored so that newer servers stay compatible.
#[derive(serde::Deserialize)]
struct MetadataSeedResponse {
    output: MetadataSeedOutput,
}

#[serde_with::serde_as]
#[derive(serde::Deserialize)]
struct MetadataSeedOutput {
    #[serde_as(as = "Option<odf::metadata::serde::yaml::datasets::Seed>")]
    #[serde(default)]
    seed: Option<odf::metadata::Seed>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
