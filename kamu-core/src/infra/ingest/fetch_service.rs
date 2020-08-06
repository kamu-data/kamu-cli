use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::infra::serde::yaml::*;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::io::prelude::*;
use std::path::Path;
use std::time::Duration;
use url::Url;

pub struct FetchService {}

impl FetchService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn fetch(
        &self,
        fetch_step: &FetchStep,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        maybe_listener: Option<&mut dyn FetchProgressListener>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let mut null_listener = NullFetchProgressListener {};
        let listener = maybe_listener.unwrap_or(&mut null_listener);

        match fetch_step {
            FetchStep::Url(ref furl) => {
                let url = Url::parse(&furl.url).map_err(|e| IngestError::internal(e))?;
                match url.scheme() {
                    "file" => self.fetch_file(
                        &url.to_file_path()
                            .map_err(|_| BadUrlError::new(&furl.url))?,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener,
                    ),
                    "http" | "https" => self.fetch_http(
                        &furl.url,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener,
                    ),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }

    // TODO: Validate event_time_source
    // TODO: Support event time from ctime/modtime
    fn fetch_file(
        &self,
        path: &Path,
        _event_time_source: Option<&EventTimeSource>,
        old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &mut dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        use fs_extra::file::*;

        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => IngestError::not_found(path, Some(e.into())),
            _ => IngestError::internal(Box::new(e)),
        })?;

        let mod_time: DateTime<Utc> = meta
            .modified()
            .map(|t| -> DateTime<Utc> { t.into() })
            .expect("File modification time is not available on this platform")
            .round_subsecs(3);

        if let Some(cp) = old_checkpoint {
            if cp.last_modified == Some(mod_time) {
                return Ok(ExecutionResult {
                    was_up_to_date: true,
                    checkpoint: cp,
                });
            }
        }

        let new_checkpoint = FetchCheckpoint {
            last_fetched: Utc::now(),
            last_modified: Some(mod_time),
            etag: None,
            source_event_time: Some(mod_time),
        };

        let mut options = CopyOptions::new();
        options.overwrite = true;

        let handle = |pr: TransitProcess| {
            let progress = FetchProgress {
                total_bytes: pr.total_bytes,
                fetched_bytes: pr.copied_bytes,
            };

            listener.on_progress(&progress);
        };

        // TODO: Use symlinks
        // TODO: Support compression
        fs_extra::file::copy_with_progress(path, target_path, &options, handle)
            .map_err(|e| IngestError::internal(e))?;

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: new_checkpoint,
        })
    }

    fn fetch_http(
        &self,
        url: &str,
        _event_time_source: Option<&EventTimeSource>,
        old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &mut dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.url(url)?;
        h.get(true)?;
        h.connect_timeout(Duration::from_secs(30))?;
        h.progress(true)?;

        if let Some(ref cp) = old_checkpoint {
            let mut list = curl::easy::List::new();
            if let Some(ref etag) = cp.etag {
                list.append(&format!("If-None-Match: {}", etag))?;
            } else if let Some(ref last_modified) = cp.last_modified {
                list.append(&format!(
                    "If-Modified-Since: {}",
                    last_modified.to_rfc2822()
                ))?;
            }
            h.http_headers(list)?;
        }

        let mut last_modified: Option<DateTime<Utc>> = None;
        let mut etag: Option<String> = None;
        {
            let mut target_file =
                std::fs::File::create(&target_path_tmp).map_err(|e| IngestError::internal(e))?;

            let mut transfer = h.transfer();

            transfer.header_function(|header| {
                let s = std::str::from_utf8(header).unwrap();
                if let Some((name, val)) = self.split_header(s) {
                    match &name.to_lowercase()[..] {
                        "last-modified" => {
                            last_modified = Some(self.parse_http_date_time(val));
                        }
                        "etag" => {
                            etag = Some(val.to_owned());
                        }
                        _ => (),
                    }
                }
                true
            })?;

            transfer.write_function(|data| {
                let written = target_file.write(data).unwrap();
                Ok(written)
            })?;

            transfer.progress_function(|f_total, f_downloaded, _, _| {
                let total = f_total as u64;
                let downloaded = f_downloaded as u64;
                if downloaded > 0 {
                    listener.on_progress(&FetchProgress {
                        total_bytes: std::cmp::max(total, downloaded),
                        fetched_bytes: downloaded,
                    });
                }
                true
            })?;

            transfer.perform().map_err(|e| {
                std::fs::remove_file(&target_path_tmp).unwrap();
                match e.code() {
                    curl_sys::CURLE_COULDNT_CONNECT => {
                        IngestError::unreachable(url, Some(e.into()))
                    }
                    _ => IngestError::internal(e),
                }
            })?;
        }

        match h.response_code()? {
            200 => {
                std::fs::rename(target_path_tmp, target_path).unwrap();
                Ok(ExecutionResult {
                    was_up_to_date: false,
                    checkpoint: FetchCheckpoint {
                        last_fetched: Utc::now(),
                        last_modified: last_modified,
                        etag: etag,
                        source_event_time: last_modified,
                    },
                })
            }
            304 => {
                std::fs::remove_file(&target_path_tmp).unwrap();
                Ok(ExecutionResult {
                    was_up_to_date: true,
                    checkpoint: old_checkpoint.unwrap(),
                })
            }
            404 => {
                std::fs::remove_file(&target_path_tmp).unwrap();
                Err(IngestError::not_found(url, None))
            }
            code => {
                std::fs::remove_file(&target_path_tmp).unwrap();
                Err(IngestError::unreachable(
                    url,
                    Some(HttpStatusError::new(code).into()),
                ))
            }
        }
    }

    fn split_header<'a>(&self, h: &'a str) -> Option<(&'a str, &'a str)> {
        if let Some(sep) = h.find(':') {
            let (name, tail) = h.split_at(sep);
            let val = tail[1..].trim();
            Some((name, val))
        } else {
            None
        }
    }

    fn parse_http_date_time(&self, val: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc2822(val)
            .unwrap_or_else(|e| panic!("Failed to parse Last-Modified header {}: {}", val, e))
            .into()
    }
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_fetched: DateTime<Utc>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub source_event_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchProgress {
    pub total_bytes: u64,
    pub fetched_bytes: u64,
}

pub trait FetchProgressListener {
    fn on_progress(&mut self, _progress: &FetchProgress) {}
}

struct NullFetchProgressListener;
impl FetchProgressListener for NullFetchProgressListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

use thiserror::Error;

#[derive(Error, Debug)]
#[error("HTTP request failed with status code {code}")]
struct HttpStatusError {
    pub code: u32,
}

impl HttpStatusError {
    fn new(code: u32) -> Self {
        Self { code: code }
    }
}

impl std::convert::From<curl::Error> for IngestError {
    fn from(e: curl::Error) -> Self {
        Self::internal(e)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Bad URL {url}")]
struct BadUrlError {
    pub url: String,
}

impl BadUrlError {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
        }
    }
}

impl std::convert::From<BadUrlError> for IngestError {
    fn from(e: BadUrlError) -> Self {
        Self::internal(e)
    }
}

///////////////////////////////////////////////////////////////////////////////
