use crate::domain::*;
use crate::infra::utils::docker_client::DockerClient;
use crate::infra::utils::docker_images;
use crate::infra::*;

use super::engine_flink::*;
use super::engine_spark::*;

use dill::*;
use slog::{o, Logger};
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

/////////////////////////////////////////////////////////////////////////////////////////

pub trait EngineFactory: Send + Sync {
    fn get_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn PullImageListener>>,
    ) -> Result<Arc<Mutex<dyn Engine>>, EngineError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineFactoryImpl {
    spark_engine: Arc<Mutex<SparkEngine>>,
    flink_engine: Arc<Mutex<FlinkEngine>>,
    container_runtime: DockerClient,
    known_images: Mutex<HashSet<String>>,
}

#[component(pub)]
impl EngineFactoryImpl {
    pub fn new(
        workspace_layout: &WorkspaceLayout,
        container_runtime: DockerClient,
        logger: Logger,
    ) -> Self {
        Self {
            spark_engine: Arc::new(Mutex::new(SparkEngine::new(
                container_runtime.clone(),
                docker_images::SPARK,
                workspace_layout,
                logger.new(o!("engine" => "spark")),
            ))),
            flink_engine: Arc::new(Mutex::new(FlinkEngine::new(
                container_runtime.clone(),
                docker_images::FLINK,
                workspace_layout,
                logger.new(o!("engine" => "flink")),
            ))),
            container_runtime: container_runtime,
            known_images: Mutex::new(HashSet::new()),
        }
    }
}

impl EngineFactory for EngineFactoryImpl {
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
            _ => Err(EngineError::not_found(engine_id)),
        }?;

        let mut known_images = self.known_images.lock().unwrap();

        if !known_images.contains(image) {
            if !self.container_runtime.has_image(image) {
                let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullPullImageListener));

                listener.begin(image);

                // TODO: Return better errors
                self.container_runtime
                    .pull_cmd(image)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()?;

                listener.success();
            }
            known_images.insert(image.to_owned());
        }

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
        Err(EngineError::not_found(engine_id))
    }
}
