// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Extension, Query};
use chrono::{DateTime, Utc};
use database_common_macros::transactional_handler;
use dill::Catalog;
use file_utils::MediaType;
use http::HeaderMap;
use http_common::*;
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::services::upload_service::{
    UploadService,
    UploadTokenBase64Json,
    UploadTokenIntoStreamError,
};
use kamu_core::*;
use time_source::SystemTimeSource;
use tokio::io::AsyncRead;

use crate::DatasetAliasInPath;
use crate::axum_utils::ensure_authenticated_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct IngestParams {
    source_name: Option<String>,

    #[param(value_type = Option<String>)]
    upload_token: Option<UploadTokenBase64Json>,
}

// TODO: SEC: Enforce a size limit on payload
// TODO: In future this handler should be putting the data into a queue (e.g.
// Kafka) and triggering a system event about new data arrival that will
// schedule an ingest task. Push ingest will be *asynchronous*. Successful
// response from this endpoint will only guarantee that the input was persisted
// in an input queue. We will offer a separate mechanism for the caller to wait
// until their data was processed. We may still provide a "synchronous" version
// of push for convenience that waits for passed data to be flushed as part of
// some block.
/// Push data ingestion
#[utoipa::path(
    post,
    path = "/ingest",
    params(IngestParams, DatasetAliasInPath),
    request_body = Vec<u8>,
    responses((status = OK, body = ())),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[transactional_handler]
pub async fn dataset_ingest_handler(
    Extension(catalog): Extension<Catalog>,
    Extension(dataset_ref): Extension<odf::DatasetRef>,
    Query(params): Query<IngestParams>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<(), ApiError> {
    let is_ingest_from_upload = params.upload_token.is_some();

    let (data_stream, media_type) = if let Some(upload_token) = params.upload_token {
        use odf::metadata::AsStackString;

        let account_id = ensure_authenticated_account(&catalog).api_err()?;
        if account_id
            .as_id_without_did_prefix()
            .as_stack_string()
            .as_str()
            != upload_token.0.owner_account_id.as_str()
        {
            return Err(ApiError::new_forbidden());
        }

        let upload_svc = catalog.get_one::<dyn UploadService>().unwrap();

        let data_stream = upload_svc
            .upload_token_into_stream(&upload_token.0)
            .await
            .map_err(|e| match e {
                UploadTokenIntoStreamError::ContentLengthMismatch(e) => ApiError::bad_request(e),
                UploadTokenIntoStreamError::ContentNotFound(e) => ApiError::not_found(e),
                UploadTokenIntoStreamError::Internal(e) => e.api_err(),
            })?;

        let media_type = upload_token
            .0
            .content_type
            .or_else(|| media_type_from_file_extension(&catalog, &upload_token.0.file_name));

        (data_stream, media_type)
    } else {
        let media_type = headers
            .get(http::header::CONTENT_TYPE)
            .map(|h| MediaType(h.to_str().unwrap().to_string()));

        let data: Box<dyn AsyncRead + Send + Unpin> =
            Box::new(crate::axum_utils::body_into_async_read(body));

        (data, media_type)
    };

    // TODO: Settle on the header name and document it
    let source_event_time: Option<DateTime<Utc>> =
        get_header(&headers, "odf-event-time", DateTime::parse_from_rfc3339)?
            .map(Into::into)
            .or_else(|| {
                let time_source = catalog.get_one::<dyn SystemTimeSource>().unwrap();
                Some(time_source.now())
            });

    // Resolve dataset
    let rebac_dataset_registry_facade =
        catalog.get_one::<dyn RebacDatasetRegistryFacade>().unwrap();

    let target_dataset = rebac_dataset_registry_facade
        .resolve_dataset_by_ref(&dataset_ref, auth::DatasetAction::Write)
        .await
        .map_err(|e| {
            use RebacDatasetRefUnresolvedError as E;
            match e {
                E::NotFound(_) | E::Access(_) => ApiError::not_found_without_reason(),
                e @ E::Internal(_) => e.int_err().api_err(),
            }
        })?;

    let push_ingest_data_use_case = catalog.get_one::<dyn PushIngestDataUseCase>().unwrap();
    push_ingest_data_use_case
        .execute(
            &target_dataset,
            DataSource::Stream(data_stream),
            PushIngestDataUseCaseOptions {
                source_name: params.source_name,
                source_event_time,
                is_ingest_from_upload,
                media_type,
                expected_head: None,
            },
            None,
        )
        .await
        .map_err(|err| match err {
            PushIngestDataError::Planning(PushIngestPlanningError::SourceNotFound(e)) => {
                ApiError::bad_request(e)
            }
            PushIngestDataError::Planning(PushIngestPlanningError::UnsupportedMediaType(_))
            | PushIngestDataError::Execution(PushIngestError::UnsupportedMediaType(_)) => {
                ApiError::new_unsupported_media_type()
            }
            PushIngestDataError::Execution(PushIngestError::ReadError(e)) => {
                ApiError::bad_request(e)
            }
            e => e.int_err().api_err(),
        })?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_header<T, E: std::error::Error + Send + Sync + 'static>(
    headers: &HeaderMap,
    key: &str,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<Option<T>, ApiError> {
    let Some(v) = headers.get(key) else {
        return Ok(None);
    };

    let v = v.to_str().map_err(ApiError::bad_request)?;

    parse(v).map(Some).map_err(ApiError::bad_request)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Consider making file name an optional parameter of the ingest to use
// for type inference
fn media_type_from_file_extension(catalog: &Catalog, file_name: &str) -> Option<MediaType> {
    let fmt_reg = catalog.get_one::<dyn DataFormatRegistry>().unwrap();

    std::path::PathBuf::from(file_name)
        .extension()
        .and_then(|s| s.to_str())
        .and_then(|ext| fmt_reg.format_by_file_extension(ext))
        .map(|fmt| fmt.media_type.to_owned())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
