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
use futures::TryStreamExt;
use kamu_core::engine::ProcessError;
use kamu_core::*;
use kamu_dataset_env_vars_services::domain::{DatasetEnvVarService, GetDatasetEnvVarError};
use opendatafabric::*;
use secrecy::ExposeSecret;
use url::Url;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_BATCH_SIZE: &str = "ODF_BATCH_SIZE";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Configs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Target number of records after which we will stop consuming from the
    /// resumable source and commit data, leaving the rest for the next
    /// iteration. This ensures that one data slice doesn't become too big.
    pub target_records_per_slice: u64,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            target_records_per_slice: 10_000,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct MqttSourceConfig {
    /// Time in milliseconds to wait for MQTT broker to send us some data after
    /// which we will consider that we have "caught up" and end the polling
    /// loop.
    pub broker_idle_timeout_ms: u64,
}

impl Default for MqttSourceConfig {
    fn default() -> Self {
        Self {
            broker_idle_timeout_ms: 1_000,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthereumSourceConfig {
    /// Default RPC endpoints to use if source does not specify one explicitly.
    pub rpc_endpoints: Vec<EthRpcEndpoint>,
    /// Default number of blocks to scan within one query to `eth_getLogs` RPC
    /// endpoint.
    pub get_logs_block_stride: u64,
    // TODO: Consider replacing this with logic that upon encountering an error still commits the
    // progress made before it
    /// Forces iteration to stop after the specified number of blocks were
    /// scanned even if we didn't reach the target record number. This is useful
    /// to not lose a lot of scanning progress in case of an RPC error.
    pub commit_after_blocks_scanned: u64,
}

impl Default for EthereumSourceConfig {
    fn default() -> Self {
        Self {
            rpc_endpoints: Vec::new(),
            get_logs_block_stride: 100_000,
            commit_after_blocks_scanned: 1_000_000,
        }
    }
}

impl EthereumSourceConfig {
    pub fn get_endpoint_by_chain_id(&self, chain_id: u64) -> Option<&EthRpcEndpoint> {
        self.rpc_endpoints.iter().find(|e| e.chain_id == chain_id)
    }
}

#[derive(Debug, Clone)]
pub struct EthRpcEndpoint {
    pub chain_id: u64,
    pub chain_name: String,
    pub node_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FetchService {
    container_runtime: Arc<ContainerRuntime>,
    source_config: Arc<SourceConfig>,
    mqtt_source_config: Arc<MqttSourceConfig>,
    eth_source_config: Arc<EthereumSourceConfig>,
    dataset_env_var_service: Arc<dyn DatasetEnvVarService>,
    run_info_dir: Arc<RunInfoDir>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
// TODO: Split this service apart into pluggable protocol implementations
impl FetchService {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        source_config: Option<Arc<SourceConfig>>,
        mqtt_source_config: Option<Arc<MqttSourceConfig>>,
        eth_source_config: Option<Arc<EthereumSourceConfig>>,
        dataset_env_var_service: Arc<dyn DatasetEnvVarService>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            container_runtime,
            source_config: source_config.unwrap_or_default(),
            mqtt_source_config: mqtt_source_config.unwrap_or_default(),
            eth_source_config: eth_source_config.unwrap_or_default(),
            dataset_env_var_service,
            run_info_dir,
        }
    }

    pub async fn fetch(
        &self,
        dataset_handle: &DatasetHandle,
        operation_id: &str,
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        maybe_listener: Option<Arc<dyn FetchProgressListener>>,
    ) -> Result<FetchResult, PollingIngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullFetchProgressListener));

        match fetch_step {
            FetchStep::Url(furl) => {
                let url = self.template_url(&furl.url, &dataset_handle.id).await?;
                let headers = self
                    .template_headers(&furl.headers, &dataset_handle.id)
                    .await?;

                match url.scheme() {
                    "file" => Self::fetch_file(
                        &url.to_file_path()
                            .map_err(|_| format!("Invalid url: {url}").int_err())?,
                        furl.event_time.as_ref(),
                        prev_source_state,
                        target_path,
                        system_time,
                        &listener,
                    ),
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
            FetchStep::FilesGlob(fglob) => Self::fetch_files_glob(
                fglob,
                prev_source_state,
                target_path,
                system_time,
                &listener,
            ),
            FetchStep::Mqtt(fetch) => {
                self.fetch_mqtt(dataset_handle, fetch, target_path, &listener)
                    .await
            }
            FetchStep::EthereumLogs(fetch) => {
                self.fetch_ethereum_logs(
                    fetch,
                    prev_source_state,
                    target_path,
                    &dataset_handle.id,
                    &listener,
                )
                .await
            }
        }
    }

    async fn template_url(
        &self,
        url_tpl: &str,
        dataset_id: &DatasetID,
    ) -> Result<Url, PollingIngestError> {
        let url = self.template_string(url_tpl, dataset_id).await?;
        Ok(Url::parse(&url).int_err()?)
    }

    async fn template_headers(
        &self,
        headers_tpl: &Option<Vec<RequestHeader>>,
        dataset_id: &DatasetID,
    ) -> Result<Vec<RequestHeader>, PollingIngestError> {
        let mut res = Vec::new();
        let empty = Vec::new();
        for htpl in headers_tpl.as_ref().unwrap_or(&empty) {
            let hdr = RequestHeader {
                name: htpl.name.clone(),
                value: self
                    .template_string(&htpl.value, dataset_id)
                    .await?
                    .into_owned(),
            };
            res.push(hdr);
        }
        Ok(res)
    }

    async fn template_string<'a>(
        &self,
        s: &'a str,
        dataset_id: &'a DatasetID,
    ) -> Result<Cow<'a, str>, PollingIngestError> {
        let mut s = Cow::from(s);
        let re_tpl = regex::Regex::new(r"\$\{\{([^}]*)\}\}").unwrap();
        let re_env = regex::Regex::new(r"^env\.([a-zA-Z-_]+)$").unwrap();

        loop {
            if let Some(ctpl) = re_tpl.captures(&s) {
                let tpl_range = ctpl.get(0).unwrap().range();

                if let Some(cenv) = re_env.captures(ctpl.get(1).unwrap().as_str().trim()) {
                    let env_name = cenv.get(1).unwrap().as_str();
                    let env_value = self
                        .dataset_env_var_service
                        .get_dataset_env_var_value_by_key_and_dataset_id(env_name, dataset_id)
                        .await
                        .map_err(|err| match err {
                            GetDatasetEnvVarError::NotFound(_) => {
                                PollingIngestError::ParameterNotFound(IngestParameterNotFound {
                                    name: env_name.to_string(),
                                })
                            }
                            GetDatasetEnvVarError::Internal(err) => {
                                PollingIngestError::Internal(err)
                            }
                        })?
                        .expose_secret()
                        .to_owned();

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
    ) -> Result<FetchResult, PollingIngestError> {
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
        let out_dir = self.run_info_dir.join(format!("fetch-{operation_id}"));
        std::fs::create_dir_all(&out_dir).int_err()?;

        let stderr_path = out_dir.join("fetch.err.txt");

        let mut target_file = tokio::fs::File::create(target_path).await.int_err()?;
        let stderr_file = std::fs::File::create(&stderr_path).int_err()?;

        let new_etag_path = out_dir.join("new-etag");
        let new_last_modified_path = out_dir.join("new-last-modified");
        let new_has_more_data_path = out_dir.join("new-has-more-data");

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
            .container_name(format!("kamu-fetch-{operation_id}"))
            .args(fetch.args.clone().unwrap_or_default())
            .volume((&out_dir, "/opt/odf/out"))
            .stdout(Stdio::piped())
            .stderr(stderr_file);

        if let Some(command) = &fetch.command {
            container_builder = container_builder.entry_point(command.join(" "));
        }

        let mut batch_size = self.source_config.target_records_per_slice;

        if let Some(env_vars) = &fetch.env {
            for EnvVar { name, value } in env_vars {
                let value = if let Some(value) = value {
                    Cow::from(value)
                } else {
                    // TODO: This is insecure: passthrough envvars
                    let value =
                        std::env::var(name).map_err(|_| IngestParameterNotFound::new(name))?;

                    Cow::from(value)
                };

                if name == ODF_BATCH_SIZE {
                    batch_size = value
                        .parse()
                        .map_err(|_| InvalidIngestParameterFormat::new(name, value))?;

                    continue;
                }

                container_builder = container_builder.environment_var(name, value);
            }
        }

        container_builder = container_builder.environment_vars([
            ("ODF_ETAG", prev_etag),
            ("ODF_LAST_MODIFIED", prev_last_modified),
            ("ODF_NEW_ETAG_PATH", "/opt/odf/out/new-etag".to_owned()),
            (
                "ODF_NEW_LAST_MODIFIED_PATH",
                "/opt/odf/out/new-last-modified".to_owned(),
            ),
            (
                "ODF_NEW_HAS_MORE_DATA_PATH",
                "/opt/odf/out/new-has-more-data".to_owned(),
            ),
            (ODF_BATCH_SIZE, batch_size.to_string()),
        ]);

        // Spawn container
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
                    target_file.flush().await.int_err()?;

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
                        .random_container_name_with_prefix("kamu-permissions-")
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

        let has_more = new_has_more_data_path.exists();

        if target_path.metadata().int_err()?.len() == 0
            && prev_source_state == source_state.as_ref()
        {
            Ok(FetchResult::UpToDate)
        } else {
            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state,
                source_event_time: None,
                has_more,
                zero_copy_path: None,
            }))
        }
    }

    fn fetch_files_glob(
        fglob: &FetchStepFilesGlob,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        match &fglob.order {
            None | Some(SourceOrdering::ByName) => (),
            Some(ord) => panic!("Files glob source can only be ordered by name, found: {ord:?}"),
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
                Err(PollingIngestError::not_found(&fglob.path, None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();

        let source_event_time = match &fglob.event_time {
            None | Some(EventTimeSource::FromSystemTime(_)) => Some(*system_time),
            Some(EventTimeSource::FromPath(src)) => {
                Some(Self::extract_event_time_from_path(&first_filename, src)?)
            }
            Some(EventTimeSource::FromMetadata(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "Files glob source does not support extracting event time fromMetadata, you \
                     should use fromPath instead",
                )
                .into());
            }
        };

        let FetchResult::Updated(fetch_res) =
            Self::fetch_file(&first_path, None, None, target_path, system_time, listener)?
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
    fn fetch_file(
        path: &Path,
        event_time_source: Option<&EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        _target_path: &Path,
        system_time: &DateTime<Utc>,
        _listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        tracing::info!(?path, "Ingesting file");

        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                PollingIngestError::not_found(path.as_os_str().to_string_lossy(), Some(e.into()))
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
            None | Some(EventTimeSource::FromMetadata(_)) => Some(mod_time),
            Some(EventTimeSource::FromSystemTime(_)) => Some(*system_time),
            Some(EventTimeSource::FromPath(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "File source does not supports fromPath event time source",
                )
                .into());
            }
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::LastModified(mod_time)),
            source_event_time,
            has_more: false,
            zero_copy_path: Some(path.to_path_buf()),
        }))
    }

    // TODO: Externalize configuration
    const HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    const HTTP_MAX_REDIRECTS: usize = 10;

    // TODO: PERF: Consider compression
    async fn fetch_http(
        &self,
        url: Url,
        headers: Vec<RequestHeader>,
        event_time_source: Option<&EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
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
            Err(err) if err.is_connect() || err.is_timeout() => Err(
                PollingIngestError::unreachable(url.as_str(), Some(err.into())),
            ),
            Err(err) => Err(err.int_err().into()),
        }?;

        match response.status() {
            http::StatusCode::OK => (),
            http::StatusCode::NOT_MODIFIED => {
                return Ok(FetchResult::UpToDate);
            }
            http::StatusCode::NOT_FOUND => {
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
            None | Some(EventTimeSource::FromMetadata(_)) => last_modified_time,
            Some(EventTimeSource::FromSystemTime(_)) => Some(*system_time),
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
    #[allow(clippy::unused_async)]
    async fn fetch_ftp(
        url: Url,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
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
                    curl_sys::CURLE_COULDNT_RESOLVE_HOST => {
                        PollingIngestError::unreachable(url.as_str(), Some(e.into()))
                    }
                    curl_sys::CURLE_COULDNT_CONNECT => {
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

    async fn fetch_mqtt(
        &self,
        dataset_handle: &DatasetHandle,
        fetch: &FetchStepMqtt,
        target_path: &Path,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        use std::io::Write as _;

        use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

        // TODO: Reconsider assignment of client identity on which QoS session data
        // rentention is based
        let client_id = format!("kamu-ingest-{}", dataset_handle.id.as_multibase());

        let mut opts = MqttOptions::new(client_id, &fetch.host, u16::try_from(fetch.port).unwrap());
        opts.set_clean_session(false);

        // TODO: Reconsider password propagation
        if let (Some(username), Some(password)) = (&fetch.username, &fetch.password) {
            let password = self.template_string(password, &dataset_handle.id).await?;
            opts.set_credentials(username, password);
        }

        tracing::debug!("Connecting to the MQTT broker and subscribing to the topic");

        let (client, mut event_loop) = AsyncClient::new(opts, 1000);
        client
            .subscribe_many(fetch.topics.clone().into_iter().map(|s| {
                rumqttc::SubscribeFilter::new(
                    s.path,
                    match s.qos {
                        None | Some(MqttQos::AtMostOnce) => QoS::AtMostOnce,
                        Some(MqttQos::AtLeastOnce) => QoS::AtLeastOnce,
                        Some(MqttQos::ExactlyOnce) => QoS::ExactlyOnce,
                    },
                )
            }))
            .await
            .int_err()?;

        let mut fetched_bytes = 0;
        let mut fetched_records = 0;
        let mut file = std::fs::File::create(target_path).int_err()?;

        let max_records = self.source_config.target_records_per_slice;
        let poll_timeout =
            std::time::Duration::from_millis(self.mqtt_source_config.broker_idle_timeout_ms);

        loop {
            // Limit number of records read if they keep flowing faster that we timeout
            // TODO: Manual ACKs to prevent lost records
            if fetched_records >= max_records {
                break;
            }

            if let Ok(poll) = tokio::time::timeout(poll_timeout, event_loop.poll()).await {
                match poll.int_err()? {
                    Event::Incoming(Packet::Publish(publish)) => {
                        // TODO: Assuming that payload is JSON and formatting it as line-delimited
                        if fetched_bytes != 0 {
                            file.write_all(b" ").int_err()?;
                        }
                        let json = std::str::from_utf8(&publish.payload).int_err()?.trim();
                        file.write_all(json.as_bytes()).int_err()?;

                        fetched_bytes += publish.payload.len() as u64 + 1;
                        fetched_records += 1;

                        listener.on_progress(&FetchProgress {
                            fetched_bytes,
                            total_bytes: TotalBytes::Unknown,
                        });
                    }
                    event => tracing::debug!(?event, "Received"),
                }
            } else {
                break;
            };
        }

        tracing::debug!(
            fetched_bytes,
            fetched_records,
            "Disconnecting from the MQTT broker"
        );

        file.flush().int_err()?;

        if fetched_bytes == 0 {
            Ok(FetchResult::UpToDate)
        } else {
            // TODO: Store last packet ID / client ID in the source state?
            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state: None,
                source_event_time: None,
                has_more: false,
                zero_copy_path: None,
            }))
        }
    }

    // TODO: FIXME: This implementation is overly complex due to DataFusion's poor
    // support of streaming / unbounded sources.
    //
    // In future datafusion-ethers should implement queries as unbounded source and
    // `DataFrame::execute_stream()` should not only yield data batches, but also
    // provide access to the state of the underlying stream for us to know how
    // far in the block range we have scanned. Since currently we can't access
    // the state of streaming - we can't interrupt the stream to resume later.
    // The implementation therefore uses DataFusion only to analyze SQL and
    // convert the WHERE clause into a filter, and then calls ETH RPC directly
    // to scan through block ranges.
    //
    // TODO: Account for re-orgs
    async fn fetch_ethereum_logs(
        &self,
        fetch: &FetchStepEthereumLogs,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        dataset_id: &DatasetID,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        use alloy::providers::{Provider, ProviderBuilder};
        use alloy::rpc::types::eth::BlockTransactionsKind;
        use datafusion::prelude::*;
        use datafusion_ethers::convert::*;
        use datafusion_ethers::stream::*;

        // Alloy does not support newlines in log signatures, but it's nice for
        // formatting
        let signature = fetch.signature.as_ref().map(|s| s.replace('\n', " "));

        let mut coder: Box<dyn Transcoder + Send> = if let Some(sig) = &signature {
            Box::new(EthRawAndDecodedLogsToArrow::new_from_signature(sig).int_err()?)
        } else {
            Box::new(EthRawLogsToArrow::new())
        };

        // Get last state
        let resume_from_state = match prev_source_state {
            None => None,
            Some(PollingSourceState::ETag(s)) => {
                let Some((num, _hash)) = s.split_once('@') else {
                    panic!("Malformed ETag: {s}");
                };
                Some(StreamState {
                    last_seen_block: num.parse().unwrap(),
                })
            }
            _ => panic!("EthereumLogs should only use ETag state"),
        };

        // Setup node RPC client
        let node_url = if let Some(url) = &fetch.node_url {
            self.template_url(url, dataset_id).await?
        } else if let Some(ep) = self
            .eth_source_config
            .get_endpoint_by_chain_id(fetch.chain_id.unwrap())
        {
            ep.node_url.clone()
        } else {
            Err(EthereumRpcError::new(format!(
                "Ethereum node RPC URL is not provided in the source manifest and no default node \
                 configured for chain ID {}",
                fetch.chain_id.unwrap()
            ))
            .int_err())?
        };

        let rpc_client = ProviderBuilder::new()
            .on_builtin(node_url.as_str())
            .await
            .int_err()?;

        let chain_id = rpc_client.get_chain_id().await.int_err()?;
        tracing::info!(%node_url, %chain_id, "Connected to ETH node");
        if let Some(expected_chain_id) = fetch.chain_id
            && expected_chain_id != chain_id
        {
            Err(EthereumRpcError::new(format!(
                "Expected to connect to chain ID {expected_chain_id} but got {chain_id} instead"
            ))
            .int_err())?;
        }

        // Setup Datafusion context
        let cfg = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let mut ctx = SessionContext::new_with_config(cfg);
        datafusion_ethers::udf::register_all(&mut ctx).unwrap();
        ctx.register_catalog(
            "eth",
            Arc::new(datafusion_ethers::provider::EthCatalog::new(
                rpc_client.clone(),
            )),
        );

        // Prepare the query according to filters
        let sql = if let Some(filter) = &fetch.filter {
            let filter = if let Some(sig) = &signature {
                format!("({filter}) and topic0 = eth_event_selector('{sig}')")
            } else {
                filter.clone()
            };
            format!("select * from eth.eth.logs where {filter}")
        } else {
            "select * from eth.eth.logs".to_string()
        };

        // Extract the filter out of the plan
        let df = ctx.sql(&sql).await.int_err()?;
        let plan = df.create_physical_plan().await.int_err()?;

        let plan_str = datafusion::physical_plan::get_plan_string(&plan).join("\n");
        tracing::info!(sql, plan = plan_str, "Original plan");

        let Some(filter) = plan
            .as_any()
            .downcast_ref::<datafusion_ethers::provider::EthGetLogs>()
            .map(|i| i.filter().clone())
        else {
            Err(EthereumRpcError::new(format!(
                "Filter is too complex and includes conditions that cannot be pushed down into \
                 RPC request. Please move such expressions into the postprocessing stage. \
                 Physical plan:\n{plan_str}"
            ))
            .int_err())?
        };

        // Resolve filter into a numeric block range
        let block_range_all = datafusion_ethers::stream::RawLogsStream::filter_to_block_range(
            &rpc_client,
            &filter.block_option,
        )
        .await
        .int_err()?;

        // Block interval that will be considered during this iteration
        let block_range_unprocessed = (
            resume_from_state
                .as_ref()
                .map_or(block_range_all.0, |s| s.last_seen_block + 1),
            block_range_all.1,
        );

        let mut state = resume_from_state.clone();

        let stream = datafusion_ethers::stream::RawLogsStream::paginate(
            rpc_client.clone(),
            filter,
            StreamOptions {
                block_stride: self.eth_source_config.get_logs_block_stride,
            },
            resume_from_state.clone(),
        );
        futures::pin_mut!(stream);

        while let Some(batch) = stream.try_next().await.int_err()? {
            let blocks_processed = batch.state.last_seen_block + 1 - block_range_unprocessed.0;

            // TODO: Design a better listener API that can reflect scanned input range,
            // number of records, and data volume transferred
            listener.on_progress(&FetchProgress {
                fetched_bytes: blocks_processed,
                total_bytes: TotalBytes::Exact(
                    block_range_unprocessed.1 + 1 - block_range_unprocessed.0,
                ),
            });

            if !batch.logs.is_empty() {
                coder.append(&batch.logs).int_err()?;
            }

            state = Some(batch.state);

            if coder.len() as u64 >= self.source_config.target_records_per_slice {
                tracing::info!(
                    target_records_per_slice = self.source_config.target_records_per_slice,
                    num_logs = coder.len(),
                    "Interrupting the stream after reaching the target batch size",
                );
                break;
            }
            if blocks_processed >= self.eth_source_config.commit_after_blocks_scanned {
                tracing::info!(
                    commit_after_blocks_scanned =
                        self.eth_source_config.commit_after_blocks_scanned,
                    blocks_processed,
                    "Interrupting the stream to commit progress",
                );
                break;
            }
        }

        // Have we made any progress?
        if resume_from_state == state {
            return Ok(FetchResult::UpToDate);
        }

        let state = state.unwrap();

        // Record scanning state
        // TODO: Reorg tolerance
        let last_seen_block = rpc_client
            .get_block(state.last_seen_block.into(), BlockTransactionsKind::Hashes)
            .await
            .int_err()?
            .unwrap();

        tracing::info!(
            blocks_scanned = state.last_seen_block - block_range_unprocessed.0 + 1,
            block_range_scanned = ?(block_range_unprocessed.0, state.last_seen_block),
            block_range_ramaining = ?(state.last_seen_block + 1, block_range_unprocessed.1),
            num_logs = coder.len(),
            "Finished block scan cycle",
        );

        // Did we exhaust the source? (not accounting for new transactions)
        let has_more = state.last_seen_block < block_range_unprocessed.1;

        // Write data, if any, to parquet file
        if coder.len() > 0 {
            let batch = coder.finish();
            {
                let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
                    std::fs::File::create_new(target_path).int_err()?,
                    batch.schema(),
                    None,
                )
                .int_err()?;
                writer.write(&batch).int_err()?;
                writer.finish().int_err()?;
            }
        }

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(format!(
                "{}@{:x}",
                last_seen_block.header.number.unwrap(),
                last_seen_block.header.hash.unwrap(),
            ))),
            source_event_time: None,
            has_more,
            zero_copy_path: None,
        }))
    }

    fn parse_http_date_time(val: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc2822(val)
            .unwrap_or_else(|e| panic!("Failed to parse Last-Modified header {val}: {e}"))
            .into()
    }

    fn extract_event_time_from_path(
        filename: &str,
        src: &EventTimeSourceFromPath,
    ) -> Result<DateTime<Utc>, PollingIngestError> {
        let time_re = regex::Regex::new(&src.pattern).int_err()?;

        let time_fmt = match src.timestamp_format {
            Some(ref fmt) => fmt,
            None => "%Y-%m-%d",
        };

        if let Some(capture) = time_re.captures(filename) {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use thiserror::Error;

#[derive(Error, Debug)]
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

impl std::convert::From<EventTimeSourceError> for PollingIngestError {
    fn from(e: EventTimeSourceError) -> Self {
        Self::Internal(e.int_err())
    }
}

#[derive(Error, Debug)]
#[error("Ethereum RPC error: {message}")]
struct EthereumRpcError {
    pub message: String,
}

impl EthereumRpcError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
