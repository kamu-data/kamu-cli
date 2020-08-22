use crate::domain::*;
use crate::infra::*;

use super::engine_flink::*;
use super::engine_spark::*;

use std::sync::{Arc, Mutex};

pub const IMAGE_SPARK: &str = "kamudata/engine-spark:0.8.1";
pub const IMAGE_FLINK: &str = "kamudata/engine-flink:0.6.0";

pub struct EngineFactory {
    spark_engine: Arc<Mutex<SparkEngine>>,
    flink_engine: Arc<Mutex<FlinkEngine>>,
}

impl EngineFactory {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            spark_engine: Arc::new(Mutex::new(SparkEngine::new(IMAGE_SPARK, workspace_layout))),
            flink_engine: Arc::new(Mutex::new(FlinkEngine::new(IMAGE_FLINK, workspace_layout))),
        }
    }

    pub fn get_engine(&mut self, engine_id: &str) -> Result<Arc<Mutex<dyn Engine>>, EngineError> {
        match engine_id {
            "sparkSQL" => Ok(self.spark_engine.clone()),
            "flink" => Ok(self.flink_engine.clone()),
            _ => Err(EngineError::not_found(engine_id)),
        }
    }
}
