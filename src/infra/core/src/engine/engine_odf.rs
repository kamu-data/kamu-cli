// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use kamu_core::*;
use odf::engine::{EngineGrpcClient, ExecuteQueryError};
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::ODFEngineConfig;
use crate::EngineIoStrategyLocalVolume;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    engine_config: ODFEngineConfig,
    image: String,
    run_info_dir: Arc<Path>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl ODFEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
        engine_config: ODFEngineConfig,
        image: &str,
        run_info_dir: Arc<Path>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            run_info_dir,
            dataset_repo,
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
}

#[async_trait::async_trait]
impl Engine for ODFEngine {
    async fn transform(&self, request: TransformRequest) -> Result<TransformResponse, EngineError> {
        let operation_id = request.operation_id.clone();
        let operation_dir = self
            .run_info_dir
            .join(format!("transform-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;
        std::fs::create_dir(&logs_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;

        let next_offset = request.next_offset;

        let io_strategy = EngineIoStrategyLocalVolume::new(self.dataset_repo.clone());
        let materialized_request = io_strategy
            .materialize_request(request, &operation_dir)
            .await
            .map_err(|e| EngineError::internal(e, Vec::new()))?;

        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            self.engine_config.clone(),
            LogsConfig::new(&logs_dir),
            &self.image,
            materialized_request.volumes,
            &operation_id,
        )
        .await?;

        let mut engine_client = engine_container.connect_client().await?;

        let engine_response = self
            .execute_query(
                &engine_container,
                &mut engine_client,
                materialized_request.engine_request,
            )
            .await;

        engine_container.terminate().await?;

        io_strategy
            .materialize_response(
                engine_response?,
                next_offset,
                materialized_request.out_data_path,
                materialized_request.out_checkpoint_path,
            )
            .await
    }
}
