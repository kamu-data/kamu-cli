// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use container_runtime::*;
use kamu_core::*;
use odf::engine::{EngineGrpcClient, ExecuteQueryError};
use odf::{ExecuteQueryInput, ExecuteQueryRequest, ExecuteQueryResponseSuccess};
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::ODFEngineConfig;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    engine_config: ODFEngineConfig,
    image: String,
    root_dir: PathBuf,
    run_info_dir: PathBuf,
}

impl ODFEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
        engine_config: ODFEngineConfig,
        image: &str,
        root_dir: PathBuf,
        run_info_dir: PathBuf,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            root_dir,
            run_info_dir,
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(container_name = engine_container.container_name()))]
    async fn execute_query(
        &self,
        engine_container: &EngineContainer,
        engine_client: &mut EngineGrpcClient,
        request: odf::ExecuteQueryRequest,
    ) -> Result<odf::ExecuteQueryResponseSuccess, EngineError> {
        tracing::info!(?request, "Performing engine operation");

        let new_checkpoint_path = request.new_checkpoint_path.clone();
        let out_data_path = request.out_data_path.clone();

        let response = engine_client.execute_query(request).await;

        tracing::info!(?response, "Operation response");

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
                    tracing::info!("Fixing up file permissions");

                    engine_container
                        .exec_shell_cmd(
                            ExecArgs::default(),
                            format!(
                                "chown -Rf {}:{} {} {}",
                                unsafe { libc::geteuid() },
                                unsafe { libc::getegid() },
                                new_checkpoint_path.display(),
                                out_data_path.display(),
                            )
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await?;
                }
            }
        }

        response.map_err(|e| match e {
            ExecuteQueryError::InvalidQuery(e) => {
                EngineError::invalid_query(e.message, engine_container.log_files())
            }
            e @ ExecuteQueryError::EngineInternalError(_) => {
                EngineError::internal(e, engine_container.log_files())
            }
            ExecuteQueryError::InternalError(e) => {
                EngineError::internal(e, engine_container.log_files())
            }
        })
    }

    fn workspace_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/workspace")
    }

    fn to_container_path(&self, host_path: &Path) -> PathBuf {
        assert!(host_path.is_absolute());
        assert!(self.root_dir.is_absolute());
        let rel = host_path.strip_prefix(&self.root_dir).unwrap();
        let joined = self.workspace_dir_in_container().join(rel);
        let unix_path = joined.to_str().unwrap().replace("\\", "/");
        PathBuf::from(unix_path)
    }
}

#[async_trait::async_trait]
impl Engine for ODFEngine {
    async fn transform(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let request_adj = ExecuteQueryRequest {
            prev_checkpoint_path: request
                .prev_checkpoint_path
                .map(|p| self.to_container_path(&p)),
            new_checkpoint_path: self.to_container_path(&request.new_checkpoint_path),
            out_data_path: self.to_container_path(&request.out_data_path),
            inputs: request
                .inputs
                .into_iter()
                .map(|input| ExecuteQueryInput {
                    data_paths: input
                        .data_paths
                        .into_iter()
                        .map(|p| self.to_container_path(&p))
                        .collect(),
                    schema_file: self.to_container_path(&input.schema_file),
                    ..input
                })
                .collect(),
            ..request
        };

        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            self.engine_config.clone(),
            LogsConfig::new(&self.run_info_dir),
            &self.image,
            // TODO: Avoid giving access to the entire workspace data
            // TODO: Use read-only permissions where possible
            vec![(self.root_dir.clone(), self.workspace_dir_in_container())],
        )
        .await?;

        let mut engine_client = engine_container.connect_client().await?;

        let res = self
            .execute_query(&engine_container, &mut engine_client, request_adj)
            .await;

        engine_container.terminate().await?;

        res
    }
}
