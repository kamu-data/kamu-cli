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

use axum::extract::{Extension, Query, TypedHeader};
use kamu::domain::*;
use opendatafabric::DatasetRef;

use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
pub struct IngestParams {
    source: Option<String>,
}

// TODO: SEC: Enforce a size limit on payload
// TODO: In future this handler should be putting the data into a queue (e.g.
// Kafka) and triggering a system event about new data arrival that will
// schedule an ingest task. Push ingest will be *asynchronous*. Successful
// response from this endpoint will only guarantee that the input was persisted
// in an input queue. We will offer a separate mechanism for the caller to wait
// until their data was processed. We may still provice a "synchronous" version
// of push for convenience that waits for passed data to be flushed as part of
// some block.
pub async fn dataset_ingest_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Extension(dataset_ref): Extension<DatasetRef>,
    Query(params): Query<IngestParams>,
    TypedHeader(content_type): TypedHeader<axum::headers::ContentType>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), ApiError> {
    let data = Box::new(crate::axum_utils::body_into_async_read(body_stream));
    let ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();

    match ingest_svc
        .ingest_from_file_stream(
            &dataset_ref,
            params.source.as_ref().map(|s| s.as_str()),
            data,
            Some(MediaType(content_type.to_string())),
            None,
        )
        .await
    {
        // Per note above, we're not including any extra information about the result
        // of the ingest operation at this point to accomodate async execution
        Ok(_) => Ok(()),
        Err(PushIngestError::SourceNotFound(e)) => Err(ApiError::bad_request(e)),
        Err(PushIngestError::UnsupportedMediaType(_)) => {
            Err(ApiError::new_unsupported_media_type())
        }
        Err(e) => Err(e.api_err()),
    }
}

/////////////////////////////////////////////////////////////////////////////////
