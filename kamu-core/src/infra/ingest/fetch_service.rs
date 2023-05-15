// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, SubsecRound, TimeZone, Utc};
use container_runtime::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;
use url::Url;

use super::*;
use crate::domain::*;

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
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        maybe_listener: Option<Arc<dyn FetchProgressListener>>,
    ) -> Result<FetchResult, IngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullFetchProgressListener));

        // TODO: Convert all implementations to non-blocking
        if !Self::requires_blocking_fetch(fetch_step)? {
            self.fetch_nonblocking(fetch_step, prev_source_state, target_path, listener)
                .await
        } else {
            let fetch_step = fetch_step.clone();
            let prev_source_state = prev_source_state.cloned();
            let target_path = target_path.to_owned();
            let container_runtime = self.container_runtime.clone();
            let container_log_dir = self.container_log_dir.clone();

            tokio::task::spawn_blocking(move || {
                Self::fetch_blocking(
                    &fetch_step,
                    prev_source_state.as_ref(),
                    &target_path,
                    listener,
                    container_runtime,
                    container_log_dir,
                )
            })
            .await
            .unwrap()
        }
    }

    fn requires_blocking_fetch(fetch_step: &FetchStep) -> Result<bool, IngestError> {
        match fetch_step {
            FetchStep::Url(furl) => {
                let url = Self::template_url(&furl.url)?;
                match url.scheme() {
                    "http" | "https" => Ok(false),
                    _ => Ok(true),
                }
            }
            _ => Ok(true),
        }
    }

    fn fetch_blocking(
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: Arc<dyn FetchProgressListener>,
        container_runtime: Arc<ContainerRuntime>,
        container_log_dir: PathBuf,
    ) -> Result<FetchResult, IngestError> {
        match &fetch_step {
            FetchStep::Url(furl) => {
                let url = Self::template_url(&furl.url)?;

                match url.scheme() {
                    "file" => Self::fetch_file(
                        &url.to_file_path()
                            .map_err(|_| format!("Invalid url: {}", url).int_err())?,
                        furl.event_time.as_ref(),
                        prev_source_state,
                        target_path,
                        listener.as_ref(),
                    ),
                    "ftp" => {
                        cfg_if::cfg_if! {
                            if #[cfg(feature = "ftp")] {
                                Self::fetch_ftp(
                                    &url,
                                    furl.event_time.as_ref(),
                                    prev_source_state,
                                    target_path,
                                    listener.as_ref(),
                                )
                            } else {
                                unimplemented!("Kamu was compiled without FTP support")
                            }
                        }
                    }
                    scheme => unimplemented!("Unsupported scheme: {}", scheme),
                }
            }
            FetchStep::FilesGlob(fglob) => {
                Self::fetch_files_glob(fglob, prev_source_state, target_path, listener.as_ref())
            }
            FetchStep::Container(fetch) => Self::fetch_container(
                container_runtime,
                container_log_dir,
                fetch,
                prev_source_state,
                target_path,
                listener,
            ),
        }
    }

    pub async fn fetch_nonblocking(
        &self,
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        match fetch_step {
            FetchStep::Url(furl) => {
                let url = Self::template_url(&furl.url)?;
                let headers = Self::template_headers(&furl.headers)?;

                match url.scheme() {
                    "http" | "https" => {
                        self.fetch_http(
                            url,
                            headers,
                            furl.event_time.as_ref(),
                            prev_source_state,
                            target_path,
                            listener.as_ref(),
                        )
                        .await
                    }
                    // TODO: Replace with proper error type
                    scheme => unimplemented!("Unsupported scheme: {}", scheme),
                }
            }
            _ => unreachable!(),
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
    #[tracing::instrument(level = "info", name = "commit", skip_all)]
    fn fetch_container(
        container_runtime: Arc<ContainerRuntime>,
        container_log_dir: PathBuf,
        fetch: &FetchStepContainer,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, IngestError> {
        use rand::Rng;

        if !container_runtime.has_image(&fetch.image) {
            Self::pull_image(
                container_runtime.as_ref(),
                &fetch.image,
                listener
                    .clone()
                    .get_pull_image_listener()
                    .unwrap_or_else(|| Arc::new(NullPullImageListener)),
            )?;
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

        let out_dir = container_log_dir.join(format!("fetch-{}", &run_id));

        std::fs::create_dir_all(&out_dir).int_err()?;

        let stderr_path = container_log_dir.join(format!("fetch-{}.err.txt", run_id));

        let stdout_file = std::fs::File::create(target_path).int_err()?;
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

        let mut environment_vars = vec![
            ("ODF_ETAG".to_owned(), prev_etag),
            ("ODF_LAST_MODIFIED".to_owned(), prev_last_modified),
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
                        Err(_) => Err(IngestParameterNotFound::new(&env_var.name)),
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
            }))
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(image))]
    fn pull_image(
        container_runtime: &ContainerRuntime,
        image: &str,
        listener: Arc<dyn PullImageListener>,
    ) -> Result<(), IngestError> {
        listener.begin(image);

        // TODO: Better error handling
        container_runtime
            .pull_cmd(image)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .int_err()?
            .exit_ok()
            .map_err(|error| {
                tracing::error!(?error, "Failed to pull ingest image");
                ImageNotFoundError::new(image)
            })?;

        tracing::info!("Successfully pulled ingest image");
        listener.success();

        Ok(())
    }

    fn fetch_files_glob(
        fglob: &FetchStepFilesGlob,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, IngestError> {
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
            Some(src) => {
                return Err(EventTimeSourceError::incompatible(format!(
                    "Files glob source only supports deriving event time from file path, found: \
                     {:?}",
                    src
                ))
                .into())
            }
        };

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

        tracing::info!(pattern = fglob.path.as_str(), last_filename = ?last_filename, matches = ?matched_files, "Matched the glob pattern");

        if matched_files.is_empty() {
            return if prev_source_state.is_some() {
                Ok(FetchResult::UpToDate)
            } else {
                Err(IngestError::not_found(&fglob.path, None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();
        let fetch_res = Self::fetch_file(&first_path, None, None, target_path, listener)?;
        assert_matches!(fetch_res, FetchResult::Updated(_));

        let source_event_time = match event_time_source {
            None => None,
            Some(src) => Some(Self::extract_event_time(&first_filename, &src)?),
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(first_filename)),
            source_event_time,
            has_more: !matched_files.is_empty(),
        }))
    }

    // TODO: Validate event_time_source
    // TODO: Support event time from ctime/modtime
    fn fetch_file(
        path: &Path,
        event_time_source: Option<&EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, IngestError> {
        use fs_extra::file::*;

        tracing::info!(path = ?path, "Ingesting file");

        match event_time_source {
            None | Some(EventTimeSource::FromMetadata) => (),
            Some(src) => {
                return Err(EventTimeSourceError::incompatible(format!(
                    concat!(
                        "File source only supports deriving event time from metadata ",
                        "(file modification time), found: {:?}"
                    ),
                    src
                ))
                .into())
            }
        };

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

        let mut options = CopyOptions::new();
        options.overwrite = true;

        let handle = |pr: TransitProcess| {
            let progress = FetchProgress {
                total_bytes: pr.total_bytes,
                fetched_bytes: pr.copied_bytes,
            };

            listener.on_progress(&progress);
        };

        // TODO: PERF: Use symlinks
        // TODO: PERF: Support compression
        fs_extra::file::copy_with_progress(path, target_path, &options, handle).int_err()?;

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::LastModified(mod_time)),
            source_event_time: Some(mod_time),
            has_more: false,
        }))
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
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, IngestError> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        use reqwest::StatusCode;
        use tokio::io::AsyncWriteExt;

        match event_time_source {
            None | Some(EventTimeSource::FromMetadata) => (),
            Some(src) => {
                return Err(EventTimeSourceError::incompatible(format!(
                    concat!(
                        "Url source only supports deriving event time from metadata ",
                        "(last modified time), found: {:?}",
                    ),
                    src
                ))
                .into())
            }
        };

        let client = reqwest::Client::builder()
            .connect_timeout(Self::HTTP_CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::limited(Self::HTTP_MAX_REDIRECTS))
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
                headers.insert(
                    reqwest::header::IF_NONE_MATCH,
                    HeaderValue::try_from(etag).unwrap(),
                );
            }
            Some(PollingSourceState::LastModified(last_modified)) => {
                headers.insert(
                    reqwest::header::IF_MODIFIED_SINCE,
                    HeaderValue::try_from(last_modified.to_rfc2822()).unwrap(),
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
            StatusCode::OK => (),
            StatusCode::NOT_MODIFIED => {
                return Ok(FetchResult::UpToDate);
            }
            StatusCode::NOT_FOUND => {
                return Err(IngestError::not_found(url.as_str(), None));
            }
            code => {
                return Err(IngestError::unreachable(
                    url.as_str(),
                    Some(HttpStatusError::new(code.as_u16() as u32).into()),
                ))
            }
        }

        let mut source_event_time = None;
        let source_state = if let Some(etag) = response.headers().get(reqwest::header::ETAG) {
            Some(PollingSourceState::ETag(
                etag.to_str().int_err()?.to_string(),
            ))
        } else if let Some(last_modified) = response.headers().get(reqwest::header::LAST_MODIFIED) {
            let last_modified = Self::parse_http_date_time(last_modified.to_str().int_err()?);
            source_event_time = Some(last_modified.clone());
            Some(PollingSourceState::LastModified(last_modified))
        } else {
            None
        };

        let total_bytes = response.content_length();
        let mut downloaded_bytes = 0;
        let mut file = tokio::fs::File::create(target_path).await.int_err()?;

        while let Some(chunk) = response.chunk().await.int_err()? {
            file.write_all(&chunk).await.int_err()?;

            downloaded_bytes += chunk.len() as u64;

            listener.on_progress(&FetchProgress {
                total_bytes: std::cmp::max(total_bytes.unwrap_or(0), downloaded_bytes),
                fetched_bytes: downloaded_bytes,
            });
        }

        // Important: Ensures file is closed immediately when dropped
        file.flush().await.int_err()?;

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state,
            source_event_time,
            has_more: false,
        }))
    }

    // TODO: not implementing caching as some FTP servers throw errors at us
    // when we request filetime :(
    #[cfg(feature = "ftp")]
    fn fetch_ftp(
        url: &Url,
        _event_time_source: Option<&EventTimeSource>,
        _prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        listener: &dyn FetchProgressListener,
    ) -> Result<FetchResult, IngestError> {
        use std::io::prelude::*;

        let target_path_tmp = target_path.with_extension("tmp");

        let mut h = curl::easy::Easy::new();
        h.connect_timeout(std::time::Duration::from_secs(30))?;
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

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: None,
            source_event_time: None,
            has_more: false,
        }))
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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum FetchResult {
    UpToDate,
    Updated(FetchResultUpdated),
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchResultUpdated {
    #[serde(default, with = "PollingSourceState")]
    pub source_state: Option<PollingSourceState>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub source_event_time: Option<DateTime<Utc>>,
    pub has_more: bool,
}

////////////////////////////////////////////////////////////////////////////////

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
        Self { code }
    }
}

#[cfg(feature = "ftp")]
impl std::convert::From<curl::Error> for IngestError {
    fn from(e: curl::Error) -> Self {
        Self::Internal(e.int_err())
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
    fn incompatible(message: String) -> EventTimeSourceError {
        EventTimeSourceError::Incompatible(EventTimeSourceIncompatibleError { message })
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
