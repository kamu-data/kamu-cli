// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use crate::infra::WorkspaceLayout;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, SubsecRound, TimeZone, Utc};
use container_runtime::*;
use std::borrow::Cow;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, info_span};
use url::Url;

pub struct FetchService {
    container_runtime: Arc<ContainerRuntime>,
    workspace_layout: Arc<WorkspaceLayout>,
}

// TODO: Reimplement with async libraries
impl FetchService {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            container_runtime,
            workspace_layout,
        }
    }

    pub async fn fetch(
        &self,
        fetch_step: &FetchStep,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        maybe_listener: Option<Arc<dyn FetchProgressListener>>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullFetchProgressListener));

        let fetch_step = fetch_step.clone();
        let target = target.to_owned();
        let container_runtime = self.container_runtime.clone();
        let workspace_layout = self.workspace_layout.clone();

        tokio::task::spawn_blocking(move || {
            Self::fetch_impl(
                &fetch_step,
                old_checkpoint,
                &target,
                listener,
                container_runtime,
                workspace_layout,
            )
        })
        .await
        .unwrap()
    }

    fn fetch_impl(
        fetch_step: &FetchStep,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        listener: Arc<dyn FetchProgressListener>,
        container_runtime: Arc<ContainerRuntime>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        match &fetch_step {
            FetchStep::Url(furl) => {
                let url = Self::template_url(&furl.url)?;
                let headers = Self::template_headers(&furl.headers)?;

                match url.scheme() {
                    "file" => Self::fetch_file(
                        &url.to_file_path()
                            .map_err(|_| format!("Invalid url: {}", url).int_err())?,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener.as_ref(),
                    ),
                    "http" | "https" => Self::fetch_http(
                        &url,
                        &headers,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener.as_ref(),
                    ),
                    "ftp" => Self::fetch_ftp(
                        &url,
                        furl.event_time.as_ref(),
                        old_checkpoint,
                        target,
                        listener.as_ref(),
                    ),
                    scheme => unimplemented!("Unsupported scheme: {}", scheme),
                }
            }
            FetchStep::FilesGlob(fglob) => {
                Self::fetch_files_glob(fglob, old_checkpoint, target, listener.as_ref())
            }
            FetchStep::Container(fetch) => Self::fetch_container(
                container_runtime,
                workspace_layout,
                fetch,
                old_checkpoint,
                target,
                listener,
            ),
        }
    }

    fn template_url(url_tpl: &str) -> Result<Url, IngestError> {
        let url = Self::template_string(url_tpl)?;
        Ok(Url::parse(&url).int_err()?)
    }

    fn template_headers(
        headers_tpl: &Option<Vec<RequestHeader>>,
    ) -> Result<Vec<RequestHeader>, IngestError> {
        let mut res = Vec::new();
        let empty = Vec::new();
        for htpl in headers_tpl.as_ref().unwrap_or(&empty) {
            let hdr = RequestHeader {
                name: htpl.name.clone(),
                value: Self::template_string(&htpl.value)?.into_owned(),
            };
            res.push(hdr);
        }
        Ok(res)
    }

    fn template_string<'a>(s: &'a str) -> Result<Cow<'a, str>, IngestError> {
        let mut s = Cow::from(s);
        let re_tpl = regex::Regex::new(r"\$\{\{([^}]*)\}\}").unwrap();
        let re_env = regex::Regex::new(r"^env\.([a-zA-Z-_]+)$").unwrap();

        loop {
            if let Some(ctpl) = re_tpl.captures(&s) {
                let tpl_range = ctpl.get(0).unwrap().range();

                if let Some(cenv) = re_env.captures(ctpl.get(1).unwrap().as_str().trim()) {
                    let env_name = cenv.get(1).unwrap().as_str();
                    let env_value = match std::env::var(env_name) {
                        Ok(v) => Ok(v),
                        Err(_) => {
                            Err(format!("Environment variable {} is not set", env_name).int_err())
                        }
                    }?;
                    s.to_mut().replace_range(tpl_range, &env_value);
                } else {
                    return Err(format!(
                        "Invalid pattern '{}' encountered in string: {}",
                        ctpl.get(0).unwrap().as_str(),
                        s
                    )
                    .int_err()
                    .into());
                }
            } else {
                if let std::borrow::Cow::Owned(_) = &s {
                    debug!(%s, "String after template substitution");
                }
                return Ok(s);
            }
        }
    }

    // TODO: Progress reporting
    // TODO: Env var security
    // TODO: Allow containers to output watermarks
    fn fetch_container(
        container_runtime: Arc<ContainerRuntime>,
        workspace_layout: Arc<WorkspaceLayout>,
        fetch: &FetchStepContainer,
        old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: Arc<dyn FetchProgressListener>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        use rand::Rng;
        use std::process::Stdio;

        if !container_runtime.has_image(&fetch.image) {
            let span = info_span!("Pulling ingest image", image_name = %fetch.image);
            let _span_guard = span.enter();

            let image_listener = listener
                .clone()
                .get_pull_image_listener()
                .unwrap_or_else(|| Arc::new(NullPullImageListener));
            image_listener.begin(&fetch.image);

            // TODO: Better error handling
            container_runtime
                .pull_cmd(&fetch.image)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .int_err()?
                .exit_ok()
                .map_err(|e| {
                    error!(error = ?e, "Failed to pull ingest image");
                    ImageNotFoundError::new(&fetch.image)
                })?;

            info!("Successfully pulled ingest image");
            image_listener.success();
        }

        listener.on_progress(&FetchProgress {
            total_bytes: 0,
            fetched_bytes: 0,
        });

        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let out_dir = workspace_layout
            .run_info_dir
            .join(format!("fetch-{}", &run_id));

        std::fs::create_dir_all(&out_dir).int_err()?;

        let stderr_path = workspace_layout
            .run_info_dir
            .join(format!("fetch-{}.err.txt", run_id));

        let stdout_file = std::fs::File::create(target_path).int_err()?;
        let stderr_file = std::fs::File::create(&stderr_path).int_err()?;

        let new_etag_path = out_dir.join("new-etag");
        let new_last_modified_path = out_dir.join("new-last-modified");

        let mut environment_vars = vec![
            (
                "ODF_LAST_MODIFIED".to_owned(),
                old_checkpoint
                    .as_ref()
                    .and_then(|c| c.last_modified)
                    .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
                    .unwrap_or_default(),
            ),
            (
                "ODF_ETAG".to_owned(),
                old_checkpoint
                    .as_ref()
                    .and_then(|c| c.etag.clone())
                    .unwrap_or_default(),
            ),
            (
                "ODF_NEW_ETAG_PATH".to_owned(),
                "/opt/odf/out/new-etag".to_owned(),
            ),
            (
                "ODF_NEW_LAST_MODIFIED_PATH".to_owned(),
                "/opt/odf/out/new-last-modified".to_owned(),
            ),
        ];

        if let Some(env) = &fetch.env {
            for env_var in env {
                if let Some(value) = &env_var.value {
                    environment_vars.push((env_var.name.clone(), value.clone()));
                } else {
                    // TODO: This is insecure
                    let value = match std::env::var(&env_var.name) {
                        Ok(value) => Ok(value),
                        Err(_) => Err(IngestInputNotFound::new(&env_var.name)),
                    }?;
                    environment_vars.push((env_var.name.clone(), value));
                }
            }
        }

        let status = container_runtime
            .run_cmd(RunArgs {
                image: fetch.image.clone(),
                entry_point: fetch.command.as_ref().map(|cmd| cmd.join(" ")),
                args: fetch.args.clone().unwrap_or_default(),
                volume_map: vec![(out_dir.clone(), PathBuf::from("/opt/odf/out"))],
                environment_vars,
                ..RunArgs::default()
            })
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .status()
            .int_err()?;

        // Fix permissions
        if container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    container_runtime
                        .run_shell_cmd(
                            RunArgs {
                                image: fetch.image.clone(),
                                volume_map: vec![(out_dir.clone(), PathBuf::from("/opt/odf/out"))],
                                user: Some("root".to_owned()),
                                ..RunArgs::default()
                            },
                            &[format!(
                                "chown -R {}:{} {}",
                                users::get_current_uid(),
                                users::get_current_gid(),
                                "/opt/odf/out"
                            )],
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status().int_err()?;
                }
            }
        }

        if !status.success() {
            return Err(ProcessError::new(status.code(), vec![stderr_path]).into());
        }

        let old_checkpoint = old_checkpoint.unwrap_or(FetchCheckpoint {
            last_fetched: Utc::now(),
            last_modified: None,
            etag: None,
            source_event_time: None,
            last_filename: None,
            has_more: false,
        });

        let etag = if new_etag_path.exists() {
            Some(std::fs::read_to_string(new_etag_path).int_err()?)
        } else {
            old_checkpoint.etag.clone()
        };

        let last_modified = if new_last_modified_path.exists() {
            let s = std::fs::read_to_string(new_last_modified_path).int_err()?;
            Some(chrono::DateTime::parse_from_rfc3339(&s).int_err()?.into())
        } else {
            old_checkpoint.last_modified.clone()
        };

        if target_path.metadata().int_err()?.len() == 0
            && etag == old_checkpoint.etag
            && last_modified == old_checkpoint.last_modified
        {
            Ok(ExecutionResult {
                was_up_to_date: true,
                checkpoint: old_checkpoint,
            })
        } else {
            Ok(ExecutionResult {
                was_up_to_date: false,
                checkpoint: FetchCheckpoint {
                    last_fetched: Utc::now(),
                    last_modified,
                    etag,
                    source_event_time: None,
                    last_filename: None,
                    has_more: false,
                },
            })
        }
    }

    fn fetch_files_glob(
        fglob: &FetchStepFilesGlob,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        listener: &dyn FetchProgressListener,
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
            .int_err()?
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

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

        info!(pattern = fglob.path.as_str(), last_filename = ?last_filename, matches = ?matched_files, "Matched the glob pattern");

        if matched_files.is_empty() {
            return if let Some(cp) = old_checkpoint {
                Ok(ExecutionResult {
                    was_up_to_date: true,
                    checkpoint: cp,
                })
            } else {
                Err(IngestError::not_found(&fglob.path, None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();
        let res = Self::fetch_file(
            &first_path,
            fglob.event_time.as_ref(),
            None,
            target,
            listener,
        );

        let event_time = match event_time_source {
            None => None,
            Some(src) => Some(Self::extract_event_time(&first_filename, &src)?),
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
        path: &Path,
        _event_time_source: Option<&EventTimeSource>,
        old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        use fs_extra::file::*;

        info!(path = ?path, "Ingesting file");

        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                IngestError::not_found(path.as_os_str().to_string_lossy(), Some(e.into()))
            }
            _ => e.int_err().into(),
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
        fs_extra::file::copy_with_progress(path, target_path, &options, handle).int_err()?;

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: new_checkpoint,
        })
    }

    fn fetch_http(
        url: &Url,
        headers: &Vec<RequestHeader>,
        _event_time_source: Option<&EventTimeSource>,
        old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.url(url.as_str())?;
        h.get(true)?;
        h.connect_timeout(Duration::from_secs(30))?;
        h.progress(true)?;
        h.follow_location(true)?;

        let mut header_list = curl::easy::List::new();
        for hdr in headers {
            header_list.append(&format!("{}: {}", hdr.name, hdr.value))?;
        }
        if let Some(ref cp) = old_checkpoint {
            if let Some(ref etag) = cp.etag {
                header_list.append(&format!("If-None-Match: {}", etag))?;
            } else if let Some(ref last_modified) = cp.last_modified {
                header_list.append(&format!(
                    "If-Modified-Since: {}",
                    last_modified.to_rfc2822()
                ))?;
            }
        }
        h.http_headers(header_list)?;

        let mut last_modified: Option<DateTime<Utc>> = None;
        let mut etag: Option<String> = None;
        {
            let mut target_file = std::fs::File::create(&target_path_tmp).int_err()?;

            let mut transfer = h.transfer();

            transfer.header_function(|header| {
                let s = std::str::from_utf8(header).unwrap();
                if let Some((name, val)) = Self::split_header(s) {
                    match &name.to_lowercase()[..] {
                        "last-modified" => {
                            last_modified = Some(Self::parse_http_date_time(val));
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
                        IngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    curl_sys::CURLE_COULDNT_CONNECT => {
                        IngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    _ => e.int_err().into(),
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
                Err(IngestError::not_found(url.as_str(), None))
            }
            code => {
                std::fs::remove_file(&target_path_tmp).unwrap();
                Err(IngestError::unreachable(
                    url.as_str(),
                    Some(HttpStatusError::new(code).into()),
                ))
            }
        }
    }

    // TODO: not implementing caching as some FTP servers throw errors at us
    // when we request filetime :(
    fn fetch_ftp(
        url: &Url,
        _event_time_source: Option<&EventTimeSource>,
        _old_checkpoint: Option<FetchCheckpoint>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<ExecutionResult<FetchCheckpoint>, IngestError> {
        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.connect_timeout(Duration::from_secs(30))?;
        h.url(url.as_str())?;
        h.progress(true)?;

        {
            let mut target_file = std::fs::File::create(&target_path_tmp).int_err()?;

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
                        IngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    curl_sys::CURLE_COULDNT_CONNECT => {
                        IngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    _ => e.int_err().into(),
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

    fn split_header<'a>(h: &'a str) -> Option<(&'a str, &'a str)> {
        if let Some(sep) = h.find(':') {
            let (name, tail) = h.split_at(sep);
            let val = tail[1..].trim();
            Some((name, val))
        } else {
            None
        }
    }

    fn parse_http_date_time(val: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc2822(val)
            .unwrap_or_else(|e| panic!("Failed to parse Last-Modified header {}: {}", val, e))
            .into()
    }

    fn extract_event_time(
        filename: &str,
        src: &EventTimeSourceFromPath,
    ) -> Result<DateTime<Utc>, IngestError> {
        let time_re = regex::Regex::new(&src.pattern).int_err()?;

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
                        let time = date.and_hms_opt(0, 0, 0).unwrap();
                        Ok(Utc.from_local_datetime(&time).unwrap())
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

pub trait FetchProgressListener: Send + Sync {
    fn on_progress(&self, _progress: &FetchProgress) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }
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
        Self::Internal(e.int_err())
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
        Self::Internal(e.int_err())
    }
}

///////////////////////////////////////////////////////////////////////////////
