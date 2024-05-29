// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use dill::Catalog;
use http::HeaderMap;
use kamu::domain::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::DatasetRef;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::api_error::*;
use crate::axum_utils::response_for_anonymous_denial;
use crate::UploadService;

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestQueryParams {
    source_name: Option<String>,
    upload_id: Option<String>,
    upload_content_type: Option<String>,
    upload_file_name: Option<String>,
}

struct IngestTaskArguments {
    data: Box<dyn AsyncRead + Send + Unpin>,
    media_type: Option<MediaType>,
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
pub async fn dataset_ingest_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Extension(dataset_ref): Extension<DatasetRef>,
    Query(params): Query<IngestQueryParams>,
    headers: HeaderMap,
    body_stream: axum::extract::BodyStream,
) -> Result<(), ApiError> {
    let arguments = if params.upload_id.is_some() {
        resolve_ready_upload_arguments(&catalog, &params).await?
    } else {
        let media_type = headers
            .get(http::header::CONTENT_TYPE)
            .map(|h| MediaType(h.to_str().unwrap().to_string()));

        let data = Box::new(crate::axum_utils::body_into_async_read(body_stream));

        IngestTaskArguments { data, media_type }
    };

    // TODO: Settle on the header name and document it
    let source_event_time: Option<DateTime<Utc>> =
        get_header(&headers, "odf-event-time", DateTime::parse_from_rfc3339)?.map(Into::into);

    let ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();
    match ingest_svc
        .ingest_from_file_stream(
            &dataset_ref,
            params.source_name.as_deref(),
            arguments.data,
            PushIngestOpts {
                media_type: arguments.media_type,
                source_event_time,
            },
            None,
        )
        .await
    {
        // Per note above, we're not including any extra information about the result
        // of the ingest operation at this point to accommodate async execution
        Ok(_) => Ok(()),
        Err(PushIngestError::ReadError(e)) => Err(ApiError::bad_request(e)),
        Err(PushIngestError::SourceNotFound(e)) => Err(ApiError::bad_request(e)),
        Err(PushIngestError::UnsupportedMediaType(_)) => {
            Err(ApiError::new_unsupported_media_type())
        }
        Err(e) => Err(e.api_err()),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn resolve_ready_upload_arguments(
    catalog: &Catalog,
    params: &IngestQueryParams,
) -> Result<IngestTaskArguments, ApiError> {
    let upload_id = params
        .upload_id
        .as_ref()
        .expect("Upload ID must be present");

    let upload_content_type = match &params.upload_content_type {
        Some(uploaded_content_type) => Ok(uploaded_content_type.clone()),
        None => Err(ApiError::bad_request(MissingUploadContentTypeError {})),
    }?;

    let upload_file_name = match &params.upload_file_name {
        Some(upload_file_name) => Ok(upload_file_name.clone()),
        None => Err(ApiError::bad_request(MissingUploadFileNameError {})),
    }?;

    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    let account_id = match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        CurrentAccountSubject::Anonymous(reason) => {
            return Err(response_for_anonymous_denial(*reason))
        }
    };

    let upload_svc = catalog.get_one::<dyn UploadService>().unwrap();
    let data = upload_svc
        .upload_reference_into_stream(&account_id, upload_id, &upload_file_name)
        .await
        .map_err(IntoApiError::api_err)?;

    Ok(IngestTaskArguments {
        data,
        media_type: Some(MediaType(upload_content_type)),
    })
}

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Missing 'uploadContentType' query parameter")]
struct MissingUploadContentTypeError {}

#[derive(Debug, Error)]
#[error("Missing 'uploadFileName' query parameter")]
struct MissingUploadFileNameError {}

/////////////////////////////////////////////////////////////////////////////////
