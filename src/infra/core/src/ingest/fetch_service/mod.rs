// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use container_runtime::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetEnvVar, DatasetKeyValueService};
use opendatafabric::*;
use url::Url;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mod configs;
pub use configs::*;

mod container;
#[cfg(feature = "ingest-evm")]
mod evm;
mod file;
#[cfg(feature = "ingest-ftp")]
mod ftp;
mod http;
#[cfg(feature = "ingest-mqtt")]
mod mqtt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_BATCH_SIZE: &str = "ODF_BATCH_SIZE";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FetchService {
    container_runtime: Arc<ContainerRuntime>,
    dataset_key_value_svc: Arc<dyn DatasetKeyValueService>,
    run_info_dir: Arc<RunInfoDir>,

    source_config: Arc<SourceConfig>,
    http_source_config: Arc<HttpSourceConfig>,

    #[cfg_attr(not(feature = "ingest-evm"), allow(dead_code))]
    eth_source_config: Arc<EthereumSourceConfig>,

    #[cfg_attr(not(feature = "ingest-mqtt"), allow(dead_code))]
    mqtt_source_config: Arc<MqttSourceConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
// TODO: Split this service apart into pluggable protocol implementations
impl FetchService {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        source_config: Option<Arc<SourceConfig>>,
        http_source_config: Option<Arc<HttpSourceConfig>>,
        mqtt_source_config: Option<Arc<MqttSourceConfig>>,
        eth_source_config: Option<Arc<EthereumSourceConfig>>,
        dataset_key_value_svc: Arc<dyn DatasetKeyValueService>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            container_runtime,
            source_config: source_config.unwrap_or_default(),
            http_source_config: http_source_config.unwrap_or_default(),
            mqtt_source_config: mqtt_source_config.unwrap_or_default(),
            eth_source_config: eth_source_config.unwrap_or_default(),
            dataset_key_value_svc,
            run_info_dir,
        }
    }

    #[allow(unused_variables)]
    pub async fn fetch(
        &self,
        dataset_handle: &DatasetHandle,
        operation_id: &str,
        fetch_step: &FetchStep,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
        maybe_listener: Option<Arc<dyn FetchProgressListener>>,
    ) -> Result<FetchResult, PollingIngestError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullFetchProgressListener));

        match fetch_step {
            FetchStep::Url(furl) => {
                let url = self.template_url(&furl.url, dataset_env_vars)?;
                let headers = self.template_headers(&furl.headers, dataset_env_vars)?;

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
                        cfg_if::cfg_if! {
                            if #[cfg(feature = "ingest-ftp")] {
                                Self::fetch_ftp(url, target_path, system_time, &listener).await
                            } else {
                                unimplemented!("Kamu was compiled without FTP support")
                            }
                        }
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
                    dataset_env_vars,
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
                cfg_if::cfg_if! {
                    if #[cfg(feature = "ingest-mqtt")] {
                    self.fetch_mqtt(
                        dataset_handle,
                        fetch,
                        target_path,
                        dataset_env_vars,
                        &listener,
                    )
                    .await
                } else {
                    unimplemented!("Kamu was compiled without MQTT support")
                }}
            }
            FetchStep::EthereumLogs(fetch) => {
                cfg_if::cfg_if! {
                        if #[cfg(feature = "ingest-evm")] {
                        self.fetch_ethereum_logs(
                            fetch,
                            prev_source_state,
                            target_path,
                            dataset_env_vars,
                            &listener,
                        )
                        .await
                    }
                    else {
                        unimplemented!("Kamu was compiled without Ethereum support")
                    }
                }
            }
        }
    }

    fn template_url(
        &self,
        url_tpl: &str,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<Url, PollingIngestError> {
        let url = self.template_string(url_tpl, dataset_env_vars)?;
        Ok(Url::parse(&url).int_err()?)
    }

    fn template_headers(
        &self,
        headers_tpl: &Option<Vec<RequestHeader>>,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<Vec<RequestHeader>, PollingIngestError> {
        let mut res = Vec::new();
        let empty = Vec::new();
        for htpl in headers_tpl.as_ref().unwrap_or(&empty) {
            let hdr = RequestHeader {
                name: htpl.name.clone(),
                value: self
                    .template_string(&htpl.value, dataset_env_vars)?
                    .into_owned(),
            };
            res.push(hdr);
        }
        Ok(res)
    }

    fn template_string<'a>(
        &self,
        s: &'a str,
        dataset_env_vars: &'a HashMap<String, DatasetEnvVar>,
    ) -> Result<Cow<'a, str>, PollingIngestError> {
        let mut s = Cow::from(s);
        let re_tpl = regex::Regex::new(r"\$\{\{([^}]*)\}\}").unwrap();
        let re_env = regex::Regex::new(r"^env\.([a-zA-Z-_]+)$").unwrap();

        loop {
            if let Some(ctpl) = re_tpl.captures(&s) {
                let tpl_range = ctpl.get(0).unwrap().range();

                if let Some(cenv) = re_env.captures(ctpl.get(1).unwrap().as_str().trim()) {
                    let env_name = cenv.get(1).unwrap().as_str();

                    let dataset_env_var_secret_value = self
                        .dataset_key_value_svc
                        .find_dataset_env_var_value_by_key(env_name, dataset_env_vars)?;

                    s.to_mut()
                        .replace_range(tpl_range, dataset_env_var_secret_value.get_exposed_value());
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

#[derive(thiserror::Error, Debug)]
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

#[derive(thiserror::Error, Debug)]
#[error("{message}")]
struct EventTimeSourceIncompatibleError {
    pub message: String,
}

#[derive(thiserror::Error, Debug)]
#[error("Bad event time pattern: {error} in pattern {pattern}")]
struct EventTimeSourcePatternError {
    pub pattern: String,
    pub error: chrono::ParseError,
}

#[derive(thiserror::Error, Debug)]
#[error("Error when extracting event time: {message}")]
struct EventTimeSourceExtractError {
    pub message: String,
}

impl std::convert::From<EventTimeSourceError> for PollingIngestError {
    fn from(e: EventTimeSourceError) -> Self {
        Self::Internal(e.int_err())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
