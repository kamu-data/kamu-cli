// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use container_runtime::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetEnvVar, DatasetKeyValueService};
use opendatafabric::*;
use url::Url;

use super::*;
use crate::ingest::fetch_service::template::template_string;
use crate::PollingSourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_BATCH_SIZE: &str = "ODF_BATCH_SIZE";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FetchService {
    pub(super) container_runtime: Arc<ContainerRuntime>,
    pub(super) dataset_key_value_svc: Arc<dyn DatasetKeyValueService>,
    pub(super) run_info_dir: Arc<RunInfoDir>,

    pub(super) source_config: Arc<SourceConfig>,
    pub(super) http_source_config: Arc<HttpSourceConfig>,

    #[cfg_attr(not(feature = "ingest-evm"), allow(dead_code))]
    pub(super) eth_source_config: Arc<EthereumSourceConfig>,

    #[cfg_attr(not(feature = "ingest-mqtt"), allow(dead_code))]
    pub(super) mqtt_source_config: Arc<MqttSourceConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Split this service apart into pluggable protocol implementations
#[dill::component(pub)]
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
                let headers = self.template_headers(furl.headers.as_ref(), dataset_env_vars)?;

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

    pub(super) fn template_url(
        &self,
        url_tpl: &str,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<Url, PollingIngestError> {
        let url = self.template_string(url_tpl, dataset_env_vars)?;
        Ok(Url::parse(&url).int_err()?)
    }

    pub(super) fn template_headers(
        &self,
        headers_tpl: Option<&Vec<RequestHeader>>,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<Vec<RequestHeader>, PollingIngestError> {
        let mut res = Vec::new();
        let empty = Vec::new();
        for htpl in headers_tpl.unwrap_or(&empty) {
            let hdr = RequestHeader {
                name: htpl.name.clone(),
                value: self.template_string(&htpl.value, dataset_env_vars)?,
            };
            res.push(hdr);
        }
        Ok(res)
    }

    pub(super) fn template_string<'a>(
        &self,
        s: &'a str,
        dataset_env_vars: &'a HashMap<String, DatasetEnvVar>,
    ) -> Result<String, PollingIngestError> {
        let lookup_fn = |env_name: &str| -> Option<String> {
            self.dataset_key_value_svc
                .find_dataset_env_var_value_by_key(env_name, dataset_env_vars)
                .ok()
                .map(|v| v.get_exposed_value().to_string())
        };
        template_string(s, &lookup_fn).map_err(PollingIngestError::TemplateError)
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
pub enum EventTimeSourceError {
    #[error(transparent)]
    Incompatible(#[from] EventTimeSourceIncompatibleError),
    #[error(transparent)]
    BadPattern(#[from] EventTimeSourcePatternError),
    #[error(transparent)]
    Extract(#[from] EventTimeSourceExtractError),
}

impl EventTimeSourceError {
    pub fn incompatible(message: impl Into<String>) -> EventTimeSourceError {
        EventTimeSourceError::Incompatible(EventTimeSourceIncompatibleError {
            message: message.into(),
        })
    }

    pub fn bad_pattern(pattern: &str, error: chrono::ParseError) -> EventTimeSourceError {
        EventTimeSourceError::BadPattern(EventTimeSourcePatternError {
            pattern: pattern.to_owned(),
            error,
        })
    }

    pub fn failed_extract(message: String) -> EventTimeSourceError {
        EventTimeSourceError::Extract(EventTimeSourceExtractError { message })
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{message}")]
pub struct EventTimeSourceIncompatibleError {
    pub message: String,
}

#[derive(thiserror::Error, Debug)]
#[error("Bad event time pattern: {error} in pattern {pattern}")]
pub struct EventTimeSourcePatternError {
    pub pattern: String,
    pub error: chrono::ParseError,
}

#[derive(thiserror::Error, Debug)]
#[error("Error when extracting event time: {message}")]
pub struct EventTimeSourceExtractError {
    pub message: String,
}

impl std::convert::From<EventTimeSourceError> for PollingIngestError {
    fn from(e: EventTimeSourceError) -> Self {
        Self::Internal(e.int_err())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
