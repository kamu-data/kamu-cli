// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use container_runtime::*;
use odf::engine::{EngineGrpcClient, ExecuteQueryError};
use odf::{ExecuteQueryInput, ExecuteQueryRequest, ExecuteQueryResponseSuccess};
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::ODFEngineConfig;
use crate::domain::*;
use crate::infra::WorkspaceLayout;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    engine_config: ODFEngineConfig,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
}

impl ODFEngine {
    const CT_VOLUME_DIR: &'static str = "/opt/engine/volume";

    pub fn new(
        container_runtime: ContainerRuntime,
        engine_config: ODFEngineConfig,
        image: &str,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            workspace_layout,
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
                                "chown -R {}:{} {}",
                                users::get_current_uid(),
                                users::get_current_gid(),
                                Self::CT_VOLUME_DIR
                            )
                        )
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
            e @ ExecuteQueryError::RpcError(_) => {
                EngineError::internal(e, engine_container.log_files())
            }
        })
    }

    fn to_container_path(&self, host_path: &Path) -> PathBuf {
        let host_path = Self::canonicalize_via_parent(host_path).unwrap();
        let datasets_path = self.workspace_layout.datasets_dir.canonicalize().unwrap();
        let repo_rel_path = host_path.strip_prefix(datasets_path).unwrap();

        let mut container_path = Self::CT_VOLUME_DIR.to_owned();
        container_path.push('/');
        container_path.push_str(&repo_rel_path.to_string_lossy());
        PathBuf::from(container_path)
    }

    fn canonicalize_via_parent(path: &Path) -> Result<PathBuf, std::io::Error> {
        match path.canonicalize() {
            Ok(p) => Ok(p),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    let mut cp = Self::canonicalize_via_parent(parent)?;
                    cp.push(path.file_name().unwrap());
                    Ok(cp)
                } else {
                    Err(e)
                }
            }
            e @ _ => e,
        }
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
            LogsConfig::new(&self.workspace_layout.run_info_dir),
            &self.image,
            vec![(
                self.workspace_layout.datasets_dir.clone(),
                PathBuf::from(Self::CT_VOLUME_DIR),
            )],
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
