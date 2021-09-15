// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::utils::docker_client::DockerClient;
use crate::infra::utils::docker_images;
use crate::infra::*;

use super::engine_flink::*;
use super::engine_odf::*;
use super::engine_spark::*;

use dill::*;
use slog::{error, info, o, Logger};
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

/////////////////////////////////////////////////////////////////////////////////////////

pub trait EngineFactory: Send + Sync {
    fn get_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn IngestEngine>>, EngineError>;

    fn get_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn Engine>>, EngineError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineFactoryImpl {
    spark_ingest_engine: Arc<Mutex<SparkEngine>>,
    spark_engine: Arc<Mutex<ODFEngine>>,
    flink_engine: Arc<Mutex<FlinkEngine>>,
    container_runtime: DockerClient,
    known_images: Mutex<HashSet<String>>,
    logger: Logger,
}

#[component(pub)]
impl EngineFactoryImpl {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        container_runtime: DockerClient,
        logger: Logger,
    ) -> Self {
        Self {
            spark_ingest_engine: Arc::new(Mutex::new(SparkEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout.as_ref(),
                logger.new(o!("engine" => "spark-ingest")),
            ))),
            spark_engine: Arc::new(Mutex::new(ODFEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout.clone(),
                logger.new(o!("engine" => "spark")),
            ))),
            flink_engine: Arc::new(Mutex::new(FlinkEngine::new(
                container_runtime.clone(),
                docker_images::FLINK,
                workspace_layout.as_ref(),
                logger.new(o!("engine" => "flink")),
            ))),
            container_runtime: container_runtime,
            known_images: Mutex::new(HashSet::new()),
            logger,
        }
    }

    fn ensure_image(
        &self,
        image: &str,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
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
            info!(self.logger, "Pulling engine image"; "image_name" => image);
            let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullPullImageListener));
            listener.begin(image);

            // TODO: Return better errors
            self.container_runtime
                    .pull_cmd(image)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()?
                    .exit_ok()
                    .map_err(|e| {
                        error!(self.logger, "Failed to pull engine image"; "image_name" => image, "error" => ?e);
                        EngineError::image_not_found(image)
                    })?;

            info!(self.logger, "Successfully pulled engine image"; "image_name" => image);
            listener.success();
            self.known_images.lock().unwrap().insert(image.to_owned());
        }

        Ok(())
    }
}

impl EngineFactory for EngineFactoryImpl {
    fn get_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn IngestEngine>>, EngineError> {
        self.ensure_image(docker_images::SPARK, maybe_listener)?;
        Ok(self.spark_ingest_engine.clone())
    }

    fn get_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn Engine>>, EngineError> {
        let (engine, image) = match engine_id {
            "spark" => Ok((
                self.spark_engine.clone() as Arc<Mutex<dyn Engine>>,
                docker_images::SPARK,
            )),
            "flink" => Ok((
                self.flink_engine.clone() as Arc<Mutex<dyn Engine>>,
                docker_images::FLINK,
            )),
            _ => Err(EngineError::image_not_found(engine_id)),
        }?;

        self.ensure_image(image, maybe_listener)?;

        Ok(engine)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineFactoryNull;

impl EngineFactory for EngineFactoryNull {
    fn get_engine(
        &self,
        engine_id: &str,
        _maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn Engine>>, EngineError> {
        Err(EngineError::image_not_found(engine_id))
    }

    fn get_ingest_engine(
        &self,
        _maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn IngestEngine>>, EngineError> {
        Err(EngineError::image_not_found("spark-ingest"))
    }
}
