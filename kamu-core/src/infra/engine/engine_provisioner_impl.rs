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

use container_runtime::{ContainerRuntime, NullPullImageListener};
use dill::*;
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tracing::info_span;
use tracing::{error, info};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerImpl {
    spark_ingest_engine: Arc<dyn IngestEngine>,
    spark_engine: Arc<dyn Engine>,
    flink_engine: Arc<dyn Engine>,
    container_runtime: ContainerRuntime,
    known_images: Mutex<HashSet<String>>,
}

#[component(pub)]
impl EngineProvisionerImpl {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        container_runtime: ContainerRuntime,
    ) -> Self {
        Self {
            spark_ingest_engine: Arc::new(SparkEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout.clone(),
            )),
            spark_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout.clone(),
            )),
            flink_engine: Arc::new(ODFEngine::new(
                container_runtime.clone(),
                docker_images::FLINK,
                workspace_layout.clone(),
            )),
            container_runtime: container_runtime,
            known_images: Mutex::new(HashSet::new()),
        }
    }

    fn ensure_image(
        &self,
        image: &str,
        listener: Arc<dyn EngineProvisioningListener>,
    ) -> Result<(), EngineError> {
        let pull_image = {
            let mut known_images = self.known_images.lock().unwrap();
            if known_images.contains(image) {
                false
            } else if self.container_runtime.has_image(image) {
                known_images.insert(image.to_owned());
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
                .status()?
                .exit_ok()
                .map_err(|e| {
                    error!(image_name = image, error = ?e, "Failed to pull engine image");
                    EngineError::image_not_found(image)
                })?;

            info!(image_name = image, "Successfully pulled engine image");
            image_listener.success();
            self.known_images.lock().unwrap().insert(image.to_owned());
        }

        Ok(())
    }
}

impl EngineProvisioner for EngineProvisionerImpl {
    fn provision_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullEngineProvisioningListener));
        self.ensure_image(docker_images::SPARK, listener.clone())?;

        listener.begin("spark-ingest");

        listener.success();
        Ok(self.spark_ingest_engine.clone())
    }

    fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineError> {
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
            _ => Err(EngineError::image_not_found(engine_id)),
        }?;

        self.ensure_image(image, listener.clone())?;

        listener.begin(engine_id);

        listener.success();
        Ok(engine)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineProvisionerNull;

impl EngineProvisioner for EngineProvisionerNull {
    fn provision_engine(
        &self,
        engine_id: &str,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineError> {
        Err(EngineError::image_not_found(engine_id))
    }

    fn provision_ingest_engine(
        &self,
        _maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineError> {
        Err(EngineError::image_not_found("spark-ingest"))
    }
}
