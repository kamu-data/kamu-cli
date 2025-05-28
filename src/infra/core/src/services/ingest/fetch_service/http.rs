// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use ::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use url::Url;

use super::*;
use crate::PollingSourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    // TODO: PERF: Consider compression
    pub(super) async fn fetch_http(
        &self,
        url: Url,
        headers: Vec<odf::metadata::RequestHeader>,
        event_time_source: Option<&odf::metadata::EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        use tokio::io::AsyncWriteExt;

        let mut client_builder = reqwest::Client::builder();

        if !self.http_source_config.user_agent.is_empty() {
            client_builder = client_builder.user_agent(&self.http_source_config.user_agent);
        }

        let client = client_builder
            .connect_timeout(self.http_source_config.connect_timeout)
            .redirect(reqwest::redirect::Policy::limited(
                self.http_source_config.max_redirects,
            ))
            .build()
            .int_err()?;

        let mut headers: HeaderMap = headers
            .into_iter()
            .map(|h| {
                let name = HeaderName::try_from(h.name).unwrap();
                let value = HeaderValue::try_from(h.value).unwrap();
                (name, value)
            })
            .collect();

        match prev_source_state {
            None => (),
            Some(PollingSourceState::ETag(etag)) => {
                headers.insert(header::IF_NONE_MATCH, HeaderValue::try_from(etag).unwrap());
            }
            Some(PollingSourceState::LastModified(last_modified)) => {
                headers.insert(
                    header::IF_MODIFIED_SINCE,
                    HeaderValue::try_from(last_modified.to_rfc2822()).unwrap(),
                );
            }
        }

        let mut response = match client.get(url.clone()).headers(headers).send().await {
            Ok(r) => Ok(r),
            Err(err) if err.is_connect() || err.is_timeout() => Err(
                PollingIngestError::unreachable(url.as_str(), Some(err.into())),
            ),
            Err(err) => Err(err.int_err().into()),
        }?;

        match response.status() {
            StatusCode::OK => (),
            StatusCode::NOT_MODIFIED => {
                return Ok(FetchResult::UpToDate);
            }
            StatusCode::NOT_FOUND => {
                return Err(PollingIngestError::not_found(url.as_str(), None));
            }
            code => {
                let body = response.text().await.ok();

                return Err(PollingIngestError::unreachable(
                    url.as_str(),
                    Some(HttpStatusError::new(u32::from(code.as_u16()), body).into()),
                ));
            }
        }

        let mut last_modified_time = None;
        let source_state = if let Some(etag) = response.headers().get(reqwest::header::ETAG) {
            Some(PollingSourceState::ETag(
                etag.to_str().int_err()?.to_string(),
            ))
        } else if let Some(last_modified) = response.headers().get(reqwest::header::LAST_MODIFIED) {
            let last_modified = Self::parse_http_date_time(last_modified.to_str().int_err()?);
            last_modified_time = Some(last_modified);
            Some(PollingSourceState::LastModified(last_modified))
        } else {
            None
        };

        let total_bytes = response
            .content_length()
            .map_or(TotalBytes::Unknown, TotalBytes::Exact);
        let mut fetched_bytes = 0;
        let mut file = tokio::fs::File::create(target_path).await.int_err()?;

        while let Some(chunk) = response.chunk().await.int_err()? {
            file.write_all(&chunk).await.int_err()?;

            fetched_bytes += chunk.len() as u64;

            listener.on_progress(&FetchProgress {
                fetched_bytes,
                total_bytes,
            });
        }

        // Important: Ensures file is closed immediately when dropped
        file.flush().await.int_err()?;

        let source_event_time = match event_time_source {
            None | Some(odf::metadata::EventTimeSource::FromMetadata(_)) => last_modified_time,
            Some(odf::metadata::EventTimeSource::FromSystemTime(_)) => Some(*system_time),
            Some(odf::metadata::EventTimeSource::FromPath(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "Url source does not support fromPath event time source",
                )
                .into());
            }
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state,
            source_event_time,
            has_more: false,
            zero_copy_path: None,
        }))
    }

    fn parse_http_date_time(val: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc2822(val)
            .unwrap_or_else(|e| panic!("Failed to parse Last-Modified header {val}: {e}"))
            .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
struct HttpStatusError {
    pub code: u32,
    pub body: Option<String>,
}

impl HttpStatusError {
    fn new(code: u32, body: Option<String>) -> Self {
        Self { code, body }
    }
}

impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP request failed with status code {}", self.code)?;
        if let Some(body) = &self.body {
            write!(f, ", message: {body}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
