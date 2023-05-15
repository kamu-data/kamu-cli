// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::utils::docker_images;
use crate::infra::*;

use super::engine_odf::*;
use super::engine_spark::*;

use container_runtime::NetworkNamespaceType;
use container_runtime::{ContainerRuntime, NullPullImageListener};
use dill::*;
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerLocal {
    config: EngineProvisionerLocalConfig,
    spark_ingest_engine: Arc<dyn IngestEngine>,
    spark_engine: Arc<dyn Engine>,
    flink_engine: Arc<dyn Engine>,
    container_runtime: ContainerRuntime,
    state: Mutex<State>,
    notify: tokio::sync::Notify,
}

struct State {
    outstanding_handles: u32,
    known_images: HashSet<String>,
}

#[component(pub)]
impl EngineProvisionerLocal {
    pub fn new(
        config: EngineProvisionerLocalConfig,
        workspace_layout: Arc<WorkspaceLayout>,
        container_runtime: ContainerRuntime,
    ) -> Self {
        let engine_config = ODFEngineConfig {
            start_timeout: config.start_timeout,
            shutdown_timeout: config.shutdown_timeout,
        };

        Self {
            spark_ingest_engine: Arc::new(SparkEngine::new(
                container_runtime.clone(),
                &config.spark_image,
                workspace_layout.clone(),
            )),
            spark_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.spark_image,
                workspace_layout.clone(),
            )),
            flink_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.flink_image,
                workspace_layout.clone(),
            )),
            container_runtime: container_runtime,
            state: Mutex::new(State {
                outstanding_handles: 0,
                known_images: HashSet::new(),
            }),
            notify: tokio::sync::Notify::new(),
            config,
        }
    }

    async fn ensure_image(
        &self,
        image: &str,
        listener: Arc<dyn EngineProvisioningListener>,
    ) -> Result<(), EngineProvisioningError> {
        let pull_image = {
            let mut state = self.state.lock().unwrap();
            if state.known_images.contains(image) {
                false
            } else if self.container_runtime.has_image(image) {
                state.known_images.insert(image.to_owned());
                false
            } else {
                true
            }
        };

        if pull_image {
            self.pull_image(
                image,
                listener
                    .get_pull_image_listener()
                    .unwrap_or_else(|| Arc::new(NullPullImageListener)),
            )
            .await?;

            {
                let mut state = self.state.lock().unwrap();
                state.known_images.insert(image.to_owned());
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(image))]
    async fn pull_image(
        &self,
        image: &str,
        listener: Arc<dyn PullImageListener>,
    ) -> Result<(), EngineProvisioningError> {
        listener.begin(image);

        let container_runtime = self.container_runtime.clone();
        let image = image.to_owned();
        // TODO: Return better errors
        tokio::task::spawn_blocking(move || {
            container_runtime
                .pull_cmd(&image)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(|e| EngineProvisioningError::internal(e))?
                .exit_ok()
                .map_err(|error| {
                    tracing::error!(?error, "Failed to pull engine image");
                    EngineProvisioningError::image_not_found(&image)
                })
        })
        .await
        .unwrap()?;

        tracing::info!("Successfully pulled engine image");
        listener.success();
        Ok(())
    }

    async fn wait_for_max_concurrency(&self) {
        let mut logged = false;

        loop {
            // Lock
            {
                let mut state = self.state.lock().unwrap();
                let max_concurrency = self.get_dynamic_max_concurrency(state.outstanding_handles);

                if state.outstanding_handles < max_concurrency {
                    state.outstanding_handles += 1;
                    break;
                }

                if !logged {
                    tracing::info!(
                        "Reached maximum concurrency of {} - waiting for an engine to be released",
                        max_concurrency
                    );
                    logged = true;
                }
            } // Unlock

            self.notify.notified().await;
        }
    }

    fn get_dynamic_max_concurrency(&self, outstanding_handles: u32) -> u32 {
        match (
            self.config.max_concurrency,
            self.container_runtime.config.network_ns,
        ) {
            (None | Some(0), NetworkNamespaceType::Host) => 1,
            // TODO: Use available memory to deretmine the optimal limit
            (None | Some(0), NetworkNamespaceType::Private) => outstanding_handles + 1,
            (Some(1), _) => 1,
            (Some(multi), NetworkNamespaceType::Private) => multi,
            (Some(multi), NetworkNamespaceType::Host) => {
                tracing::warn!(
                    "Ingoring specified engine max concurrency of {} since running in the Host \
                     networking mode",
                    multi
                );
                1
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl EngineProvisioner for EngineProvisionerLocal {
    async fn provision_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineProvisioningError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));
        self.ensure_image(&self.config.spark_image, listener.clone())
            .await?;

        listener.begin("spark-ingest");
        self.wait_for_max_concurrency().await;
        listener.success();

        Ok(IngestEngineHandle::new(
            self,
            self.spark_ingest_engine.clone(),
        ))
    }

    async fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineProvisioningError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));

        let (engine, image) = match engine_id {
            "spark" => Ok((
                self.spark_engine.clone() as Arc<dyn Engine>,
                &self.config.spark_image,
            )),
            "flink" => Ok((
                self.flink_engine.clone() as Arc<dyn Engine>,
                &self.config.flink_image,
            )),
            _ => Err(EngineProvisioningError::image_not_found(engine_id)),
        }?;

        self.ensure_image(image, listener.clone()).await?;

        listener.begin(engine_id);
        self.wait_for_max_concurrency().await;
        listener.success();

        Ok(EngineHandle::new(self, engine))
    }

    fn release_engine(&self, engine: &dyn Engine) {
        tracing::info!("Releasing the engine {:p}", engine);

        {
            let mut state = self.state.lock().unwrap();
            state.outstanding_handles -= 1;
            self.notify.notify_one();
        }
    }

    fn release_ingest_engine(&self, engine: &dyn IngestEngine) {
        tracing::info!("Releasing the engine {:p}", engine);

        {
            let mut state = self.state.lock().unwrap();
            state.outstanding_handles -= 1;
            self.notify.notify_one();
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Config
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EngineProvisionerLocalConfig {
    /// Maximum number of engine handles given out any single time
    pub max_concurrency: Option<u32>,
    /// Timeout for starting engine container
    pub start_timeout: Duration,
    /// Timeout for waiting for engine container to shutdown cleanly
    pub shutdown_timeout: Duration,

    // TODO: Remove in favor of explicit images in ODF protocol
    pub spark_image: String,
    pub flink_image: String,
}

// This is for tests only
impl Default for EngineProvisionerLocalConfig {
    fn default() -> Self {
        Self {
            max_concurrency: None,
            start_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            spark_image: docker_images::SPARK.to_owned(),
            flink_image: docker_images::FLINK.to_owned(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Null Object
/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerNull;

#[async_trait::async_trait(?Send)]
impl EngineProvisioner for EngineProvisionerNull {
    async fn provision_engine(
        &self,
        engine_id: &str,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineProvisioningError> {
        Err(EngineProvisioningError::image_not_found(engine_id))
    }

    async fn provision_ingest_engine(
        &self,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineProvisioningError> {
        Err(EngineProvisioningError::image_not_found("spark-ingest"))
    }

    fn release_engine(&self, _engine: &dyn Engine) {}
    fn release_ingest_engine(&self, _engine: &dyn IngestEngine) {}
}
