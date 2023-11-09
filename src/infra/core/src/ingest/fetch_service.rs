// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use chrono::{DateTime, SubsecRound, TimeZone, Utc};
use container_runtime::*;
use kamu_core::engine::ProcessError;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use super::*;

////////////////////////////////////////////////////////////////////////////////

pub struct FetchService {
    container_runtime: Arc<ContainerRuntime>,
    container_log_dir: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////

// TODO: Split this service apart into pluggable protocol implementations
impl FetchService {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        container_log_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            container_runtime,
            container_log_dir: container_log_dir.into(),
        }
    }

    pub async fn fetch(
        &self,
        operation_id: &str,
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        maybe_listener: Option<Arc<dyn FetchProgressListener>>,
    ) -> Result<FetchResult, IngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullFetchProgressListener));

        match fetch_step {
            FetchStep::Url(furl) => {
                let url = Self::template_url(&furl.url)?;
                let headers = Self::template_headers(&furl.headers)?;

                match url.scheme() {
                    "file" => {
                        Self::fetch_file(
                            &url.to_file_path()
                                .map_err(|_| format!("Invalid url: {}", url).int_err())?,
                            furl.event_time.as_ref(),
                            prev_source_state,
                            target_path,
                            system_time,
                            &listener,
                        )
                        .await
                    }
                    "http" | "https" => {
                        self.fetch_http(
                            url,
                            headers,
                            furl.event_time.as_ref(),
                            prev_source_state,
                            target_path,
                            system_time,
                            &listener,
                        )
                        .await
                    }
                    "ftp" | "ftps" => {
                        Self::fetch_ftp(url, target_path, system_time, &listener).await
                    }
                    // TODO: Replace with proper error type
                    scheme => unimplemented!("Unsupported scheme: {}", scheme),
                }
            }
            FetchStep::Container(fetch) => {
                self.fetch_container(
                    operation_id,
                    fetch,
                    prev_source_state,
                    target_path,
                    &listener,
                )
                .await
            }
            FetchStep::FilesGlob(fglob) => {
                Self::fetch_files_glob(
                    fglob,
                    prev_source_state,
                    target_path,
                    system_time,
                    &listener,
                )
                .await
            }
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
                    tracing::debug!(%s, "String after template substitution");
                }
                return Ok(s);
            }
        }
    }

    // TODO: Progress reporting
    // TODO: Env var security
    // TODO: Allow containers to output watermarks
    async fn fetch_container(
        &self,
        operation_id: &str,
        fetch: &FetchStepContainer,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        // Pull image
        let pull_image_listener = listener
            .clone()
            .get_pull_image_listener()
            .unwrap_or_else(|| Arc::new(NullPullImageListener));

        self.container_runtime
            .ensure_image(&fetch.image, Some(pull_image_listener.as_ref()))
            .await?;

        listener.on_progress(&FetchProgress {
            fetched_bytes: 0,
            total_bytes: TotalBytes::Unknown,
        });

        // Setup logging
        let out_dir = self
            .container_log_dir
            .join(format!("fetch-{}", operation_id));
        std::fs::create_dir_all(&out_dir).int_err()?;

        let stderr_path = self.container_log_dir.join("fetch.err.txt");

        let mut target_file = tokio::fs::File::create(target_path).await.int_err()?;
        let stderr_file = std::fs::File::create(&stderr_path).int_err()?;

        let new_etag_path = out_dir.join("new-etag");
        let new_last_modified_path = out_dir.join("new-last-modified");

        let (prev_etag, prev_last_modified) = match prev_source_state {
            None => (String::new(), String::new()),
            Some(PollingSourceState::ETag(etag)) => (etag.clone(), String::new()),
            Some(PollingSourceState::LastModified(last_modified)) => (
                String::new(),
                last_modified.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            ),
        };

        let mut container_builder = self
            .container_runtime
            .run_attached(&fetch.image)
            .container_name(format!("kamu-fetch-{}", operation_id))
            .args(fetch.args.clone().unwrap_or_default())
            .environment_vars([
                ("ODF_ETAG", prev_etag),
                ("ODF_LAST_MODIFIED", prev_last_modified),
                ("ODF_NEW_ETAG_PATH", "/opt/odf/out/new-etag".to_owned()),
                (
                    "ODF_NEW_LAST_MODIFIED_PATH",
                    "/opt/odf/out/new-last-modified".to_owned(),
                ),
            ])
            .volume((&out_dir, "/opt/odf/out"))
            .stdout(Stdio::piped())
            .stderr(stderr_file);

        if let Some(command) = &fetch.command {
            container_builder = container_builder.entry_point(command.join(" "));
        }

        if let Some(env) = &fetch.env {
            for env_var in env {
                if let Some(value) = &env_var.value {
                    container_builder = container_builder.environment_var(&env_var.name, value);
                } else {
                    // TODO: This is insecure
                    let value = match std::env::var(&env_var.name) {
                        Ok(value) => Ok(value),
                        Err(_) => Err(IngestParameterNotFound::new(&env_var.name)),
                    }?;
                    container_builder = container_builder.environment_var(&env_var.name, value);
                }
            }
        }

        // Spawh container
        let mut container = container_builder.spawn().int_err()?;

        // Handle output in a task
        let mut stdout = container.take_stdout().unwrap();
        let listener = listener.clone();

        let output_task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut fetched_bytes = 0;
            let mut buf = [0; 1024];

            loop {
                let read = stdout.read(&mut buf).await.int_err()?;
                if read == 0 {
                    break;
                }

                fetched_bytes += read as u64;
                listener.on_progress(&FetchProgress {
                    fetched_bytes,
                    total_bytes: TotalBytes::Unknown,
                });

                target_file.write_all(&buf[..read]).await.int_err()?;
            }

            Ok::<(), InternalError>(())
        });

        // Wait for container to exit
        let status = container.wait().await.int_err()?;
        output_task.await.int_err()?.int_err()?;

        // Fix permissions
        if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    self.container_runtime
                        .run_attached(&fetch.image)
                        .shell_cmd(format!(
                            "chown -Rf {}:{} {}",
                            unsafe { libc::geteuid() },
                            unsafe { libc::getegid() },
                            "/opt/odf/out"
                        ))
                        .user("root")
                        .volume((out_dir, "/opt/odf/out"))
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await
                        .int_err()?;
                }
            }
        }

        if !status.success() {
            return Err(ProcessError::new(status.code(), vec![stderr_path]).into());
        }

        let source_state = if new_etag_path.exists() {
            let s = std::fs::read_to_string(new_etag_path).int_err()?;
            Some(PollingSourceState::ETag(s))
        } else if new_last_modified_path.exists() {
            let s = std::fs::read_to_string(new_last_modified_path).int_err()?;
            Some(PollingSourceState::LastModified(
                chrono::DateTime::parse_from_rfc3339(&s).int_err()?.into(),
            ))
        } else {
            None
        };

        if target_path.metadata().int_err()?.len() == 0
            && prev_source_state == source_state.as_ref()
        {
            Ok(FetchResult::UpToDate)
        } else {
            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state,
                source_event_time: None,
                has_more: false,
                zero_copy_path: None,
            }))
        }
    }

    async fn fetch_files_glob(
        fglob: &FetchStepFilesGlob,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        match &fglob.order {
            None => (),
            Some(SourceOrdering::ByName) => (),
            Some(ord) => panic!(
                "Files glob source can only be ordered by name, found: {:?}",
                ord
            ),
        }

        let last_filename = match prev_source_state {
            Some(PollingSourceState::ETag(etag)) => Some(etag),
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
                if let Some(lfn) = last_filename {
                    name > lfn
                } else {
                    true
                }
            })
            .collect();

        matched_files.sort_by(|a, b| b.0.cmp(&a.0));

        tracing::info!(
            pattern = fglob.path.as_str(),
            ?last_filename,
            matches = ?matched_files,
            "Matched the glob pattern"
        );

        if matched_files.is_empty() {
            return if prev_source_state.is_some() {
                Ok(FetchResult::UpToDate)
            } else {
                Err(IngestError::not_found(&fglob.path, None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();

        let source_event_time = match &fglob.event_time {
            None | Some(EventTimeSource::FromSystemTime) => Some(system_time.clone()),
            Some(EventTimeSource::FromPath(src)) => {
                Some(Self::extract_event_time_from_path(&first_filename, &src)?)
            }
            Some(EventTimeSource::FromMetadata) => {
                return Err(EventTimeSourceError::incompatible(
                    "Files glob source does not support extracting event time fromMetadata, you \
                     should use fromPath instead",
                )
                .into());
            }
        };

        let FetchResult::Updated(fetch_res) =
            Self::fetch_file(&first_path, None, None, target_path, system_time, listener).await?
        else {
            panic!("Glob rule should be caching individual files")
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(first_filename)),
            source_event_time,
            has_more: !matched_files.is_empty(),
            zero_copy_path: fetch_res.zero_copy_path,
        }))
    }

    // TODO: Validate event_time_source
    // TODO: Support event time from ctime/modtime
    // TODO: Resolve symlinks
    // TODO: PERF: Consider compression
    async fn fetch_file(
        path: &Path,
        event_time_source: Option<&EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        tracing::info!(?path, "Ingesting file");

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

        if let Some(PollingSourceState::LastModified(last_modified)) = prev_source_state {
            if *last_modified == mod_time {
                return Ok(FetchResult::UpToDate);
            }
        }

        let source_event_time = match event_time_source {
            None | Some(EventTimeSource::FromMetadata) => Some(mod_time),
            Some(EventTimeSource::FromSystemTime) => Some(system_time.clone()),
            Some(EventTimeSource::FromPath(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "File source does not supports fromPath event time source",
                )
                .into());
            }
        };

        // Heuristics for STDIN (/dev/fd/0) and other special device files
        if meta.is_file() {
            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state: Some(PollingSourceState::LastModified(mod_time)),
                source_event_time,
                has_more: false,
                zero_copy_path: Some(path.to_path_buf()),
            }))
        } else {
            // Copy data into the cache, not to confuse the engines
            let total_bytes = TotalBytes::Exact(meta.len());
            let mut source = std::fs::File::open(path).int_err()?;
            let mut target = std::fs::File::create(target_path).int_err()?;
            let listener = listener.clone();

            tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
                use std::io::{Read, Write};

                let mut buf = [0u8; 1024];
                let mut fetched_bytes = 0;

                loop {
                    let read = source.read(&mut buf)?;
                    if read == 0 {
                        break;
                    }

                    target.write_all(&buf[..read])?;

                    fetched_bytes += read as u64;
                    listener.on_progress(&FetchProgress {
                        fetched_bytes,
                        total_bytes,
                    });
                }

                Ok(())
            })
            .await
            .int_err()?
            .int_err()?;

            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state: Some(PollingSourceState::LastModified(mod_time)),
                source_event_time,
                has_more: false,
                zero_copy_path: None,
            }))
        }
    }

    // TODO: Externalize configuration
    const HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    const HTTP_MAX_REDIRECTS: usize = 10;

    async fn fetch_http(
        &self,
        url: Url,
        headers: Vec<RequestHeader>,
        event_time_source: Option<&EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        use tokio::io::AsyncWriteExt;

        let client = reqwest::Client::builder()
            .connect_timeout(Self::HTTP_CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::limited(Self::HTTP_MAX_REDIRECTS))
            .build()
            .int_err()?;

        let mut headers: http::HeaderMap = headers
            .into_iter()
            .map(|h| {
                let name = http::HeaderName::try_from(h.name).unwrap();
                let value = http::HeaderValue::try_from(h.value).unwrap();
                (name, value)
            })
            .collect();

        match prev_source_state {
            None => (),
            Some(PollingSourceState::ETag(etag)) => {
                headers.insert(
                    http::header::IF_NONE_MATCH,
                    http::HeaderValue::try_from(etag).unwrap(),
                );
            }
            Some(PollingSourceState::LastModified(last_modified)) => {
                headers.insert(
                    http::header::IF_MODIFIED_SINCE,
                    http::HeaderValue::try_from(last_modified.to_rfc2822()).unwrap(),
                );
            }
        }

        let mut response = match client.get(url.clone()).headers(headers).send().await {
            Ok(r) => Ok(r),
            Err(err) if err.is_connect() || err.is_timeout() => {
                Err(IngestError::unreachable(url.as_str(), Some(err.into())))
            }
            Err(err) => Err(err.int_err().into()),
        }?;

        match response.status() {
            http::StatusCode::OK => (),
            http::StatusCode::NOT_MODIFIED => {
                return Ok(FetchResult::UpToDate);
            }
            http::StatusCode::NOT_FOUND => {
                return Err(IngestError::not_found(url.as_str(), None));
            }
            code => {
                return Err(IngestError::unreachable(
                    url.as_str(),
                    Some(HttpStatusError::new(code.as_u16() as u32).into()),
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
            last_modified_time = Some(last_modified.clone());
            Some(PollingSourceState::LastModified(last_modified))
        } else {
            None
        };

        let total_bytes = response
            .content_length()
            .map(|b| TotalBytes::Exact(b))
            .unwrap_or(TotalBytes::Unknown);
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
            None | Some(EventTimeSource::FromMetadata) => last_modified_time,
            Some(EventTimeSource::FromSystemTime) => Some(system_time.clone()),
            Some(EventTimeSource::FromPath(_)) => {
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

    #[allow(unused_variables)]
    async fn fetch_ftp(
        url: Url,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "ftp")] {
                let target_path = target_path.to_owned();
                let system_time = system_time.clone();
                let listener = listener.clone();
                tokio::task::spawn_blocking(move || {
                        Self::fetch_ftp_impl(
                            url,
                            &target_path,
                            system_time,
                            listener.as_ref(),
                        )
                    })
                    .await
                    .int_err()?
            } else {
                unimplemented!("Kamu was compiled without FTP support")
            }
        }
    }

    // TODO: convert to non-blocking
    // TODO: not implementing caching as some FTP servers throw errors at us
    // when we request filetime :(
    #[cfg(feature = "ftp")]
    fn fetch_ftp_impl(
        url: Url,
        target_path: &Path,
        system_time: DateTime<Utc>,
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, IngestError> {
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

        std::fs::rename(target_path_tmp, target_path).int_err()?;

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: None,
            source_event_time: Some(system_time),
            has_more: false,
            zero_copy_path: None,
        }))
    }

    fn parse_http_date_time(val: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc2822(val)
            .unwrap_or_else(|e| panic!("Failed to parse Last-Modified header {}: {}", val, e))
            .into()
    }

    fn extract_event_time_from_path(
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
                match DateTime::parse_from_str(group.as_str(), time_fmt) {
                    Ok(dt) => Ok(dt.into()),
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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchResult {
    UpToDate,
    Updated(FetchResultUpdated),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResultUpdated {
    pub source_state: Option<PollingSourceState>,
    pub source_event_time: Option<DateTime<Utc>>,
    pub has_more: bool,
    pub zero_copy_path: Option<PathBuf>,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchProgress {
    pub fetched_bytes: u64,
    pub total_bytes: TotalBytes,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TotalBytes {
    Unknown,
    Exact(u64),
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
        Self { code }
    }
}

#[derive(Error, Debug)]
enum EventTimeSourceError {
    #[error(transparent)]
    Incompatible(#[from] EventTimeSourceIncompatibleError),
    #[error(transparent)]
    BadPattern(#[from] EventTimeSourcePatternError),
    #[error(transparent)]
    Extract(#[from] EventTimeSourceExtractError),
}

impl EventTimeSourceError {
    fn incompatible(message: impl Into<String>) -> EventTimeSourceError {
        EventTimeSourceError::Incompatible(EventTimeSourceIncompatibleError {
            message: message.into(),
        })
    }

    fn bad_pattern(pattern: &str, error: chrono::ParseError) -> EventTimeSourceError {
        EventTimeSourceError::BadPattern(EventTimeSourcePatternError {
            pattern: pattern.to_owned(),
            error,
        })
    }

    fn failed_extract(message: String) -> EventTimeSourceError {
        EventTimeSourceError::Extract(EventTimeSourceExtractError { message })
    }
}

#[derive(Error, Debug)]
#[error("{message}")]
struct EventTimeSourceIncompatibleError {
    pub message: String,
}

#[derive(Error, Debug)]
#[error("Bad event time pattern: {error} in pattern {pattern}")]
struct EventTimeSourcePatternError {
    pub pattern: String,
    pub error: chrono::ParseError,
}

#[derive(Error, Debug)]
#[error("Error when extracting event time: {message}")]
struct EventTimeSourceExtractError {
    pub message: String,
}

impl std::convert::From<EventTimeSourceError> for IngestError {
    fn from(e: EventTimeSourceError) -> Self {
        Self::Internal(e.int_err())
    }
}

///////////////////////////////////////////////////////////////////////////////
