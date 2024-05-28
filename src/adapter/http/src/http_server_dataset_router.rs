// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::response::{IntoResponse, Response};
use axum::Json;
use http::StatusCode;
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use opendatafabric as odf;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::simple_protocol::*;
use crate::{
    AccessToken,
    DatasetAuthorizationLayer,
    DatasetResolverLayer,
    MakeUploadContextError,
    SaveUploadError,
    UploadService,
};

/////////////////////////////////////////////////////////////////////////////////

// Extractor of dataset identity for single-tenant smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: odf::DatasetName,
}

// Extractor of account + dataset identity for multi-tenant smart transfer
// protocol
#[derive(serde::Deserialize)]
struct DatasetByAccountAndName {
    account_name: odf::AccountName,
    dataset_name: odf::DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////

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

pub async fn platform_login_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    Json(payload): Json<LoginRequestBody>,
) -> Response {
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
            Json(json!(response_body)).into_response()
        }
        Err(e) => match e {
            kamu_accounts::LoginError::UnsupportedMethod(e) => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            kamu_accounts::LoginError::InvalidCredentials(e) => {
                (StatusCode::UNAUTHORIZED, e.to_string()).into_response()
            }
            kamu_accounts::LoginError::RejectedCredentials(e) => {
                (StatusCode::UNAUTHORIZED, e.to_string()).into_response()
            }
            kamu_accounts::LoginError::DuplicateCredentials => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            kamu_accounts::LoginError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
            }
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
pub async fn platform_token_validate_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
) -> axum::response::Response {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();

    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(_) => axum::response::Response::builder()
            .status(http::StatusCode::OK)
            .body(Default::default())
            .unwrap(),
        CurrentAccountSubject::Anonymous(reason) => response_for_anonymous_denial(*reason),
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFileUploadQuery {
    file_name: String,
    content_length: usize,
}

pub async fn platform_file_upload_get_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Query(query): axum::extract::Query<PlatformFileUploadQuery>,
) -> Response {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    let account_id = match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        CurrentAccountSubject::Anonymous(reason) => return response_for_anonymous_denial(*reason),
    };

    let access_token = catalog.get_one::<AccessToken>().unwrap();

    let upload_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_service
        .make_upload_context(
            &account_id,
            query.file_name,
            query.content_length,
            access_token.as_ref(),
        )
        .await
    {
        Ok(upload_context) => Json(json!(upload_context)).into_response(),
        Err(e) => match e {
            MakeUploadContextError::TooLarge(_) => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            MakeUploadContextError::Internal(e) => {
                tracing::error!("{e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
            }
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
pub struct UploadFromPath {
    upload_id: String,
    file_name: String,
}

#[allow(clippy::unused_async)]
pub async fn platform_file_upload_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
    axum::TypedHeader(content_length): axum::TypedHeader<axum::headers::ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Response {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    let account_id = match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        CurrentAccountSubject::Anonymous(reason) => return response_for_anonymous_denial(*reason),
    };

    let file_data = Box::new(crate::axum_utils::body_into_async_read(body_stream));

    let upload_local_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_local_service
        .save_upload(
            &account_id,
            upload_param.upload_id,
            upload_param.file_name,
            usize::try_from(content_length.0).unwrap(),
            file_data,
        )
        .await
    {
        Ok(_) => (StatusCode::OK, "").into_response(),
        Err(e) => match e {
            SaveUploadError::TooLarge(_) => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            SaveUploadError::Internal(_) | SaveUploadError::NotSupported(_) => {
                tracing::error!("{e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
            }
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////

fn response_for_anonymous_denial(reason: AnonymousAccountReason) -> Response {
    match reason {
        AnonymousAccountReason::AuthenticationExpired => {
            (StatusCode::UNAUTHORIZED, "Authentication token expired").into_response()
        }
        AnonymousAccountReason::AuthenticationInvalid => {
            (StatusCode::UNAUTHORIZED, "Authentication token invalid").into_response()
        }
        AnonymousAccountReason::NoAuthenticationProvided => {
            (StatusCode::UNAUTHORIZED, "No authentication token provided").into_response()
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

fn is_dataset_optional_for_request(request: &http::Request<hyper::Body>) -> bool {
    request.uri().path() == "/push"
}

/////////////////////////////////////////////////////////////////////////////////

fn get_dataset_action_for_request(
    request: &http::Request<hyper::Body>,
) -> kamu::domain::auth::DatasetAction {
    if !request.method().is_safe() || request.uri().path() == "/push" {
        kamu::domain::auth::DatasetAction::Write
    } else {
        kamu::domain::auth::DatasetAction::Read
    }
}

/////////////////////////////////////////////////////////////////////////////////
