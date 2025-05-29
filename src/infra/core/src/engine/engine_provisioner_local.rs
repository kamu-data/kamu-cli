// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use container_runtime::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::engine::*;
use kamu_core::*;

use super::engine_odf::*;
use crate::utils::docker_images;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerLocal {
    config: EngineProvisionerLocalConfig,
    spark_engine: Arc<dyn Engine>,
    flink_engine: Arc<dyn Engine>,
    datafusion_engine: Arc<dyn Engine>,
    risingwave_engine: Arc<dyn Engine>,
    container_runtime: Arc<ContainerRuntime>,
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    notify: tokio::sync::Notify,
}

struct State {
    outstanding_handles: u32,
    known_images: HashSet<String>,
}

#[dill::component(pub)]
#[dill::interface(dyn EngineProvisioner)]
impl EngineProvisionerLocal {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        config: EngineProvisionerLocalConfig,
        container_runtime: Arc<ContainerRuntime>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        let engine_config = ODFEngineConfig {
            start_timeout: config.start_timeout,
            shutdown_timeout: config.shutdown_timeout,
        };

        Self {
            spark_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.spark_image,
                run_info_dir.clone(),
            )),
            flink_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.flink_image,
                run_info_dir.clone(),
            )),
            datafusion_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.datafusion_image,
                run_info_dir.clone(),
            )),
            risingwave_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                engine_config.clone(),
                &config.risingwave_image,
                run_info_dir.clone(),
            )),
            container_runtime,
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    outstanding_handles: 0,
                    known_images: HashSet::new(),
                }),
                notify: tokio::sync::Notify::new(),
            }),
            config,
        }
    }

    async fn ensure_image(
        &self,
        image: &str,
        listener: Arc<dyn EngineProvisioningListener>,
    ) -> Result<(), EngineProvisioningError> {
        let mut pull_image = {
            let state = self.inner.state.lock().unwrap();
            !state.known_images.contains(image)
        };

        if pull_image && self.container_runtime.has_image(image).await.int_err()? {
            let mut state = self.inner.state.lock().unwrap();
            state.known_images.insert(image.to_owned());
            pull_image = false;
        }

        if pull_image {
            let listener = listener
                .get_pull_image_listener()
                .unwrap_or_else(|| Arc::new(NullPullImageListener));

            self.container_runtime
                .pull_image(image, Some(listener.as_ref()))
                .await?;
            {
                let mut state = self.inner.state.lock().unwrap();
                state.known_images.insert(image.to_owned());
            }
        }

        Ok(())
    }

    async fn wait_for_max_concurrency(&self) {
        let mut logged = false;

        loop {
            // Lock
            {
                let mut state = self.inner.state.lock().unwrap();
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

            self.inner.notify.notified().await;
        }
    }

    fn get_dynamic_max_concurrency(&self, outstanding_handles: u32) -> u32 {
        match (
            self.config.max_concurrency,
            self.container_runtime.config.network_ns,
        ) {
            (None | Some(0), NetworkNamespaceType::Host) | (Some(1), _) => 1,
            // TODO: Use available memory to determine the optimal limit
            (None | Some(0), NetworkNamespaceType::Private) => outstanding_handles + 1,
            (Some(multi), NetworkNamespaceType::Private) => multi,
            (Some(multi), NetworkNamespaceType::Host) => {
                tracing::warn!(
                    "Ignoring specified engine max concurrency of {} since running in the Host \
                     networking mode",
                    multi
                );
                1
            }
        }
    }

    /// Called when [EngineHandle] is dropped
    fn release_engine(inner: &Inner, engine: &dyn Engine) {
        tracing::info!("Releasing the engine {:p}", engine);

        {
            let mut state = inner.state.lock().unwrap();
            state.outstanding_handles -= 1;
            inner.notify.notify_one();
        }
    }
}

#[async_trait::async_trait]
impl EngineProvisioner for EngineProvisionerLocal {
    async fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<Arc<dyn Engine>, EngineProvisioningError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));

        let (engine, image) = match engine_id {
            "spark" => Ok((self.spark_engine.clone(), &self.config.spark_image)),
            "flink" => Ok((self.flink_engine.clone(), &self.config.flink_image)),
            "datafusion" => Ok((
                self.datafusion_engine.clone(),
                &self.config.datafusion_image,
            )),
            "risingwave" => Ok((
                self.risingwave_engine.clone(),
                &self.config.risingwave_image,
            )),
            _ => Err(format!("Unsupported engine {engine_id}").int_err()),
        }?;

        self.ensure_image(image, listener.clone()).await?;

        listener.begin(engine_id);
        self.wait_for_max_concurrency().await;
        listener.success();

        Ok(Arc::new(EngineHandle::new(self.inner.clone(), engine)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    pub datafusion_image: String,
    pub risingwave_image: String,
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
            datafusion_image: docker_images::DATAFUSION.to_owned(),
            risingwave_image: docker_images::RISINGWAVE.to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Null Object
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn EngineProvisioner)]
pub struct EngineProvisionerNull;

#[async_trait::async_trait]
impl EngineProvisioner for EngineProvisionerNull {
    async fn provision_engine(
        &self,
        _engine_id: &str,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<Arc<dyn Engine>, EngineProvisioningError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EngineHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wraps an engine use to decrease concurrent use counter when engine is
/// dropped
struct EngineHandle {
    inner: Arc<Inner>,
    engine: Arc<dyn Engine>,
}

impl EngineHandle {
    pub fn new(inner: Arc<Inner>, engine: Arc<dyn Engine>) -> Self {
        Self { inner, engine }
    }
}

#[async_trait::async_trait]
impl Engine for EngineHandle {
    async fn execute_raw_query(
        &self,
        request: RawQueryRequestExt,
    ) -> Result<RawQueryResponseExt, EngineError> {
        self.engine.execute_raw_query(request).await
    }

    async fn execute_transform(
        &self,
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformResponseExt, EngineError> {
        self.engine.execute_transform(request, datasets_map).await
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        EngineProvisionerLocal::release_engine(&self.inner, self.engine.as_ref());
    }
}
