use crate::domain::*;
use crate::infra::utils::docker_client::DockerClient;
use crate::infra::utils::docker_images;
use crate::infra::*;

use super::engine_flink::*;
use super::engine_spark::*;

use slog::{o, Logger};
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

pub struct EngineFactory {
    spark_engine: Arc<Mutex<SparkEngine>>,
    flink_engine: Arc<Mutex<FlinkEngine>>,
    docker_client: DockerClient,
    known_images: HashSet<String>,
}

impl EngineFactory {
    pub fn new(workspace_layout: &WorkspaceLayout, logger: Logger) -> Self {
        Self {
            spark_engine: Arc::new(Mutex::new(SparkEngine::new(
                docker_images::SPARK,
                workspace_layout,
                logger.new(o!("engine" => "spark")),
            ))),
            flink_engine: Arc::new(Mutex::new(FlinkEngine::new(
                docker_images::FLINK,
                workspace_layout,
                logger.new(o!("engine" => "flink")),
            ))),
            docker_client: DockerClient::new(),
            known_images: HashSet::new(),
        }
    }

    pub fn get_engine(
        &mut self,
        engine_id: &str,
        maybe_listener: Option<&mut dyn PullImageListener>,
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

        if !self.known_images.contains(image) {
            if !self.docker_client.has_image(image) {
                let mut null_listener = NullPullImageListener;
                let listener = maybe_listener.unwrap_or(&mut null_listener);

                listener.begin(image);

                // TODO: Return better errors
                self.docker_client
                    .pull_cmd(image)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()?;

                listener.success();
            }
            self.known_images.insert(image.to_owned());
        }

        Ok(engine)
    }
}
