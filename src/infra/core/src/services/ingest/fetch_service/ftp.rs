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

use chrono::{DateTime, Utc};
use internal_error::*;
use kamu_core::*;
use url::Url;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    pub(super) async fn fetch_ftp(
        url: Url,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        let target_path = target_path.to_owned();
        let system_time = *system_time;
        let listener = listener.clone();
        tokio::task::spawn_blocking(move || {
            Self::fetch_ftp_impl(&url, &target_path, system_time, listener.as_ref())
        })
        .await
        .int_err()?
    }

    // TODO: convert to non-blocking
    // TODO: not implementing caching as some FTP servers throw errors at us
    // when we request filetime :(
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    fn fetch_ftp_impl(
        url: &Url,
        target_path: &Path,
        system_time: DateTime<Utc>,
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, PollingIngestError> {
        use std::io::prelude::*;

        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.connect_timeout(std::time::Duration::from_secs(30))
            .int_err()?;
        h.url(url.as_str()).int_err()?;
        h.progress(true).int_err()?;

        {
            let mut target_file = std::fs::File::create(&target_path_tmp).int_err()?;

            let mut transfer = h.transfer();

            transfer
                .write_function(|data| {
                    let written = target_file.write(data).unwrap();
                    Ok(written)
                })
                .int_err()?;

            transfer
                .progress_function(|f_total, f_downloaded, _, _| {
                    let total_bytes = f_total as u64;
                    let fetched_bytes = f_downloaded as u64;
                    if fetched_bytes > 0 {
                        listener.on_progress(&FetchProgress {
                            fetched_bytes,
                            total_bytes: TotalBytes::Exact(std::cmp::max(
                                total_bytes,
                                fetched_bytes,
                            )),
                        });
                    }
                    true
                })
                .int_err()?;

            transfer.perform().map_err(|e| {
                std::fs::remove_file(&target_path_tmp).unwrap();
                match e.code() {
                    curl_sys::CURLE_COULDNT_RESOLVE_HOST | curl_sys::CURLE_COULDNT_CONNECT => {
                        PollingIngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    _ => e.int_err().into(),
                }
            })?;
        }

        std::fs::rename(target_path_tmp, target_path).int_err()?;

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: None,
            source_event_time: Some(system_time),
            has_more: false,
            zero_copy_path: None,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
