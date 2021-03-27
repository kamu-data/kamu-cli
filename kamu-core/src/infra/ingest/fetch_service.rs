use super::*;
use crate::domain::*;
use opendatafabric::serde::yaml::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, SubsecRound, TimeZone, Utc};
use slog::{info, Logger};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

pub struct FetchService {
    logger: Logger,
}

impl FetchService {
    pub fn new(logger: Logger) -> Self {
        Self { logger: logger }
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
                    "ftp" => self.fetch_ftp(
                        &furl.url,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener,
                    ),
                    scheme => unimplemented!("Unsupported scheme: {}", scheme),
                }
            }
            FetchStep::FilesGlob(ref fglob) => {
                self.fetch_files_glob(fglob, old_checkpoint, target, listener)
            }
        }
    }

    fn fetch_files_glob(
        &self,
        fglob: &FetchStepFilesGlob,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        listener: &mut dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        match &fglob.order {
            None => (),
            Some(SourceOrdering::ByName) => (),
            Some(ord) => panic!(
                "Files glob source can only be ordered by name, found: {:?}",
                ord
            ),
        }

        let event_time_source = match &fglob.event_time {
            None => None,
            Some(EventTimeSource::FromPath(fp)) => Some(fp),
            Some(src) => panic!(
                "Files glob source only supports deriving event time from file path, found: {:?}",
                src
            ),
        };

        let last_filename = match old_checkpoint {
            Some(FetchCheckpoint {
                last_filename: Some(ref name),
                ..
            }) => Some(name.clone()),
            _ => None,
        };

        let matched_paths = glob::glob(&fglob.path)
            .map_err(|e| IngestError::internal(e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| IngestError::internal(e))?;

        let mut matched_files: Vec<(String, PathBuf)> = matched_paths
            .into_iter()
            .filter(|p| p.is_file())
            .map(|p| (p.file_name().unwrap().to_str().unwrap().to_owned(), p))
            .filter(|(name, _)| {
                if let Some(ref lfn) = last_filename {
                    name > lfn
                } else {
                    true
                }
            })
            .collect();

        matched_files.sort_by(|a, b| b.0.cmp(&a.0));

        info!(self.logger, "Matched the glob pattern"; "pattern" => &fglob.path, "last_filename" => &last_filename, "matches" => ?matched_files);

        if matched_files.is_empty() {
            return if let Some(cp) = old_checkpoint {
                Ok(ExecutionResult {
                    was_up_to_date: true,
                    checkpoint: cp,
                })
            } else {
                Err(IngestError::not_found(fglob.path.clone(), None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();
        let res = self.fetch_file(
            &first_path,
            fglob.event_time.as_ref(),
            None,
            target,
            listener,
        );

        let event_time = match event_time_source {
            None => None,
            Some(src) => Some(self.extract_event_time(&first_filename, &src)?),
        };

        match res {
            Ok(exec_res) => Ok(ExecutionResult {
                was_up_to_date: exec_res.was_up_to_date,
                checkpoint: FetchCheckpoint {
                    last_fetched: exec_res.checkpoint.last_fetched,
                    source_event_time: event_time,
                    last_filename: Some(first_filename),
                    has_more: !matched_files.is_empty(),
                    etag: None,
                    last_modified: None,
                },
            }),
            Err(e) => Err(e),
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

        info!(self.logger, "Ingesting file"; "path" => ?path);

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
            last_filename: None,
            has_more: false,
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
        h.follow_location(true)?;

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
                    curl_sys::CURLE_COULDNT_RESOLVE_HOST => {
                        IngestError::unreachable(url, Some(e.into()))
                    }
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
                        last_filename: None,
                        has_more: false,
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

    // TODO: not implementing caching as some FTP servers throw errors at us
    // when we request filetime :(
    fn fetch_ftp(
        &self,
        url: &str,
        _event_time_source: Option<&EventTimeSource>,
        _old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &mut dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.connect_timeout(Duration::from_secs(30))?;
        h.url(url)?;
        h.progress(true)?;

        {
            let mut target_file =
                std::fs::File::create(&target_path_tmp).map_err(|e| IngestError::internal(e))?;

            let mut transfer = h.transfer();

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
                    curl_sys::CURLE_COULDNT_RESOLVE_HOST => {
                        IngestError::unreachable(url, Some(e.into()))
                    }
                    curl_sys::CURLE_COULDNT_CONNECT => {
                        IngestError::unreachable(url, Some(e.into()))
                    }
                    _ => IngestError::internal(e),
                }
            })?;
        }

        std::fs::rename(target_path_tmp, target_path).unwrap();
        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: FetchCheckpoint {
                last_fetched: Utc::now(),
                last_modified: None,
                etag: None,
                source_event_time: None,
                last_filename: None,
                has_more: false,
            },
        })
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

    fn extract_event_time(
        &self,
        filename: &str,
        src: &EventTimeSourceFromPath,
    ) -> Result<DateTime<Utc>, IngestError> {
        let time_re = regex::Regex::new(&src.pattern).map_err(|e| IngestError::internal(e))?;

        let time_fmt = match src.timestamp_format {
            Some(ref fmt) => fmt,
            None => "%Y-%m-%d",
        };

        if let Some(capture) = time_re.captures(&filename) {
            if let Some(group) = capture.get(1) {
                match Utc.datetime_from_str(group.as_str(), time_fmt) {
                    Ok(dt) => Ok(dt),
                    Err(_) => {
                        let date = chrono::NaiveDate::parse_from_str(group.as_str(), time_fmt)
                            .map_err(|e| EventTimeSourceError::bad_pattern(time_fmt, e))?;
                        Ok(Utc.from_local_date(&date).and_hms_opt(0, 0, 0).unwrap())
                    }
                }
            } else {
                Err(EventTimeSourceError::failed_extract(format!(
                    "Pattern {} doesn't have group 1",
                    src.pattern
                ))
                .into())
            }
        } else {
            Err(EventTimeSourceError::failed_extract(format!(
                "Failed to match pattern {} to {}",
                src.pattern, filename
            ))
            .into())
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_fetched: DateTime<Utc>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub source_event_time: Option<DateTime<Utc>>,
    pub last_filename: Option<String>,
    pub has_more: bool,
}

impl FetchCheckpoint {
    pub fn is_cacheable(&self) -> bool {
        self.last_modified.is_some() || self.etag.is_some() || self.last_filename.is_some()
    }
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

#[derive(Error, Debug)]
enum EventTimeSourceError {
    #[error("Bad event time pattern: {0}")]
    BadPattern(#[from] EventTimeSourcePatternError),
    #[error("Error when extracting event time: {0}")]
    Extract(#[from] EventTimeSourceExtractError),
}

impl EventTimeSourceError {
    fn bad_pattern(pattern: &str, error: chrono::ParseError) -> EventTimeSourceError {
        EventTimeSourceError::BadPattern(EventTimeSourcePatternError {
            pattern: pattern.to_owned(),
            error: error,
        })
    }

    fn failed_extract(message: String) -> EventTimeSourceError {
        EventTimeSourceError::Extract(EventTimeSourceExtractError { message: message })
    }
}

#[derive(Error, Debug)]
#[error("{error} in pattern {pattern}")]
struct EventTimeSourcePatternError {
    pub pattern: String,
    pub error: chrono::ParseError,
}

#[derive(Error, Debug)]
#[error("{message}")]
struct EventTimeSourceExtractError {
    pub message: String,
}

impl std::convert::From<EventTimeSourceError> for IngestError {
    fn from(e: EventTimeSourceError) -> Self {
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
