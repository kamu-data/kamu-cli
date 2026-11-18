// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Extension, Query};
use axum::response::Json;
use database_common::{TransactionRef, transaction};
use dill::specs::{Maybe, OneOf};
use http_common::ApiError;
use internal_error::*;
use setty::types::DurationString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeRequest {
    #[serde(default)]
    db_transaction: Option<bool>,

    #[serde(default)]
    sleep: Option<DurationString>,

    #[serde(default)]
    error: Option<ProbeError>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResponse {}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProbeError {
    Panic,
    Internal,
    Api,
}

pub async fn probe_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Query(req): Query<ProbeRequest>,
) -> Result<Json<ProbeResponse>, ApiError> {
    transaction!(
        #[catalog(catalog)]
        #[when(req.db_transaction.unwrap_or(false))]
        |cat| -> Result<_, ApiError> {
            if let Some(tx) = cat.get::<Maybe<OneOf<TransactionRef>>>().int_err()? {
                // Open DB transaction manually, as normally it's allocated lazily
                tx.begin().await?;
            }

            if let Some(sleep) = req.sleep {
                tokio::time::sleep(sleep.into()).await;
            }

            match req.error {
                None => (),
                Some(ProbeError::Panic) => panic!("Probe panic"),
                Some(ProbeError::Internal) => return Err("Probe internal error".int_err().into()),
                Some(ProbeError::Api) => {
                    return Err(ApiError::bad_request_with_message("Probe API error"));
                }
            }

            Ok(Json(ProbeResponse {}))
        }
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
