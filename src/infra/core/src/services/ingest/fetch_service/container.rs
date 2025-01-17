// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use internal_error::*;
use kamu_core::engine::ProcessError;
use kamu_core::*;
use kamu_datasets::DatasetEnvVar;

use super::*;
use crate::PollingSourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    // TODO: Progress reporting
    // TODO: Env var security
    // TODO: Allow containers to output watermarks
    pub(super) async fn fetch_container(
        &self,
        operation_id: &str,
        fetch: &odf::metadata::FetchStepContainer,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        // Pull image
        let pull_image_listener = listener
            .clone()
            .get_pull_image_listener()
            .unwrap_or_else(|| Arc::new(NullPullImageListener));

        let image = self.template_string(&fetch.image, dataset_env_vars)?;

        self.container_runtime
            .ensure_image(&image, Some(pull_image_listener.as_ref()))
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
            .run_attached(&image)
            .container_name(format!("kamu-fetch-{operation_id}"))
            .volume((&out_dir, "/opt/odf/out"))
            .stdout(Stdio::piped())
            .stderr(stderr_file);

        if let Some(command) = &fetch.command {
            container_builder = container_builder.entry_point(command.join(" "));
        }

        if let Some(args) = &fetch.args {
            container_builder = container_builder.args(
                args.iter()
                    .map(|arg| self.template_string(arg, dataset_env_vars))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        let mut batch_size = self.source_config.target_records_per_slice;

        if let Some(env_vars) = &fetch.env {
            for odf::metadata::EnvVar { name, value } in env_vars {
                let value = if let Some(value) = value {
                    self.template_string(value, dataset_env_vars)?
                } else {
                    let value = self
                        .dataset_key_value_svc
                        .find_dataset_env_var_value_by_key(name, dataset_env_vars)?;

                    value.into_exposed_value()
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
                        .run_attached(&image)
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

        if source_state.is_some() && source_state.as_ref() == prev_source_state {
            if target_path.metadata().int_err()?.len() != 0 {
                Err(ContainerProtocolError::new(
                    "Source state didn't change but some output data was returned",
                )
                .int_err())?;
            }
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Container protocol error: {message}")]
struct ContainerProtocolError {
    pub message: String,
}

impl ContainerProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
