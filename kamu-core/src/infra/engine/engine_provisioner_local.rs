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
use std::sync::Condvar;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info_span;
use tracing::warn;
use tracing::{error, info};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerLocal {
    config: EngineProvisionerLocalConfig,
    spark_ingest_engine: Arc<dyn IngestEngine>,
    spark_engine: Arc<dyn Engine>,
    flink_engine: Arc<dyn Engine>,
    container_runtime: ContainerRuntime,
    state: Mutex<State>,
    condvar: Condvar,
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
            config,
            spark_ingest_engine: Arc::new(SparkEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout.clone(),
            )),
            spark_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                docker_images::SPARK,
                workspace_layout.clone(),
            )),
            flink_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                docker_images::FLINK,
                workspace_layout.clone(),
            )),
            container_runtime: container_runtime,
            state: Mutex::new(State {
                outstanding_handles: 0,
                known_images: HashSet::new(),
            }),
            condvar: Condvar::new(),
        }
    }

    fn ensure_image(
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
            let span = info_span!("Pulling engine image", image_name = image);
            let _span_guard = span.enter();

            let image_listener = listener
                .get_pull_image_listener()
                .unwrap_or_else(|| Arc::new(NullPullImageListener));
            image_listener.begin(image);

            // TODO: Return better errors
            self.container_runtime
                .pull_cmd(image)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(|e| EngineProvisioningError::internal(e))?
                .exit_ok()
                .map_err(|e| {
                    error!(image_name = image, error = ?e, "Failed to pull engine image");
                    EngineProvisioningError::image_not_found(image)
                })?;

            info!(image_name = image, "Successfully pulled engine image");
            image_listener.success();

            {
                let mut state = self.state.lock().unwrap();
                state.known_images.insert(image.to_owned());
            }
        }

        Ok(())
    }

    fn wait_for_max_concurrency(&self) {
        let mut state = self.state.lock().unwrap();
        let mut max_concurrency = self.get_dynamic_max_concurrency(state.outstanding_handles);

        while state.outstanding_handles == max_concurrency {
            info!(
                "Reached maximum concurrency of {} - waiting for an engine to be released",
                max_concurrency
            );
            state = self.condvar.wait(state).unwrap();
            max_concurrency = self.get_dynamic_max_concurrency(state.outstanding_handles);
        }

        state.outstanding_handles += 1;
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
                warn!("Ingoring specified engine max concurrency of {} since running in the Host networking mode", multi);
                1
            }
        }
    }
}

impl EngineProvisioner for EngineProvisionerLocal {
    fn provision_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineProvisioningError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));
        self.ensure_image(docker_images::SPARK, listener.clone())?;

        listener.begin("spark-ingest");
        self.wait_for_max_concurrency();
        listener.success();

        Ok(IngestEngineHandle::new(
            self,
            self.spark_ingest_engine.clone(),
        ))
    }

    fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineProvisioningError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));

        let (engine, image) = match engine_id {
            "spark" => Ok((
                self.spark_engine.clone() as Arc<dyn Engine>,
                docker_images::SPARK,
            )),
            "flink" => Ok((
                self.flink_engine.clone() as Arc<dyn Engine>,
                docker_images::FLINK,
            )),
            _ => Err(EngineProvisioningError::image_not_found(engine_id)),
        }?;

        self.ensure_image(image, listener.clone())?;

        listener.begin(engine_id);
        self.wait_for_max_concurrency();
        listener.success();

        Ok(EngineHandle::new(self, engine))
    }

    fn release_engine(&self, engine: &dyn Engine) {
        info!("Releasing the engine {:p}", engine);

        {
            let mut state = self.state.lock().unwrap();
            state.outstanding_handles -= 1;
            self.condvar.notify_one();
        }
    }

    fn release_ingest_engine(&self, engine: &dyn IngestEngine) {
        info!("Releasing the engine {:p}", engine);

        {
            let mut state = self.state.lock().unwrap();
            state.outstanding_handles -= 1;
            self.condvar.notify_one();
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
}

// This is for tests only
impl Default for EngineProvisionerLocalConfig {
    fn default() -> Self {
        Self {
            max_concurrency: None,
            start_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Null Object
/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerNull;

impl EngineProvisioner for EngineProvisionerNull {
    fn provision_engine(
        &self,
        engine_id: &str,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineProvisioningError> {
        Err(EngineProvisioningError::image_not_found(engine_id))
    }

    fn provision_ingest_engine(
        &self,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineProvisioningError> {
        Err(EngineProvisioningError::image_not_found("spark-ingest"))
    }

    fn release_engine(&self, _engine: &dyn Engine) {}
    fn release_ingest_engine(&self, _engine: &dyn IngestEngine) {}
}
