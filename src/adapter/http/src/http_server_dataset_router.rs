// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::{Extension, Json};
use database_common_macros::transactional_handler;
use dill::Catalog;
use opendatafabric as odf;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api_error::{ApiError, IntoApiError, ResultIntoApiError};
use crate::axum_utils::ensure_authenticated_account;
use crate::simple_protocol::*;
use crate::{DatasetAuthorizationLayer, DatasetResolverLayer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extractor of dataset identity for single-tenant smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: odf::DatasetName,
}

/// Extractor of account + dataset identity for multi-tenant smart transfer
/// protocol
#[derive(serde::Deserialize)]
struct DatasetByAccountAndName {
    account_name: odf::AccountName,
    dataset_name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn smart_transfer_protocol_router() -> axum::Router {
    axum::Router::new()
        .route("/refs/:reference", axum::routing::get(dataset_refs_handler))
        .route(
            "/blocks/:block_hash",
            axum::routing::get(dataset_blocks_handler),
        )
        .route(
            "/data/:physical_hash",
            axum::routing::get(dataset_data_get_handler).put(dataset_data_put_handler),
        )
        .route(
            "/checkpoints/:physical_hash",
            axum::routing::get(dataset_checkpoints_get_handler)
                .put(dataset_checkpoints_put_handler),
        )
        .route("/pull", axum::routing::get(dataset_pull_ws_upgrade_handler))
        .route("/push", axum::routing::get(dataset_push_ws_upgrade_handler))
        .layer(DatasetAuthorizationLayer::new(
            get_dataset_action_for_request,
        ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn add_dataset_resolver_layer(
    dataset_router: axum::Router,
    multi_tenant: bool,
) -> axum::Router {
    use axum::extract::Path;

    if multi_tenant {
        dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByAccountAndName>| {
                odf::DatasetAlias::new(Some(p.account_name), p.dataset_name).into_local_ref()
            },
            is_dataset_optional_for_request,
        ))
    } else {
        dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByName>| p.dataset_name.as_local_ref(),
            is_dataset_optional_for_request,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequestBody {
    pub login_method: String,
    pub login_credentials_json: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponseBody {
    pub access_token: String,
}

#[transactional_handler]
pub async fn platform_login_handler(
    Extension(catalog): Extension<Catalog>,
    Json(payload): Json<LoginRequestBody>,
) -> Result<Json<Value>, ApiError> {
    let authentication_service = catalog
        .get_one::<dyn kamu_accounts::AuthenticationService>()
        .unwrap();

    let login_result = authentication_service
        .login(
            payload.login_method.as_str(),
            payload.login_credentials_json,
        )
        .await;

    match login_result {
        Ok(login_response) => {
            let response_body = LoginResponseBody {
                access_token: login_response.access_token,
            };
            Ok(Json(json!(response_body)))
        }
        Err(e) => Err(match e {
            kamu_accounts::LoginError::UnsupportedMethod(e) => ApiError::bad_request(e),
            kamu_accounts::LoginError::InvalidCredentials(e) => ApiError::new_unauthorized_from(e),
            kamu_accounts::LoginError::RejectedCredentials(e) => ApiError::new_unauthorized_from(e),
            kamu_accounts::LoginError::DuplicateCredentials => ApiError::bad_request(e),
            kamu_accounts::LoginError::Internal(e) => e.api_err(),
        }),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
pub async fn platform_token_validate_handler(catalog: Extension<Catalog>) -> Result<(), ApiError> {
    ensure_authenticated_account(&catalog).api_err()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn is_dataset_optional_for_request(request: &http::Request<hyper::Body>) -> bool {
    request.uri().path() == "/push"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_dataset_action_for_request(
    request: &http::Request<hyper::Body>,
) -> kamu::domain::auth::DatasetAction {
    if !request.method().is_safe() || request.uri().path() == "/push" {
        kamu::domain::auth::DatasetAction::Write
    } else {
        kamu::domain::auth::DatasetAction::Read
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
