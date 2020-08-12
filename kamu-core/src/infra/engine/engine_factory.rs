use crate::domain::*;
use crate::infra::*;

use super::spark_engine::*;

use std::sync::{Arc, Mutex};

pub const IMAGE_SPARK: &str = "kamudata/engine-spark:0.8.0";

pub struct EngineFactory {
    spark_engine: Arc<Mutex<SparkEngine>>,
}

impl EngineFactory {
    pub fn new(workspace_layout: &WorkspaceLayout, volume_layout: &VolumeLayout) -> Self {
        Self {
            spark_engine: Arc::new(Mutex::new(SparkEngine::new(
                IMAGE_SPARK,
                workspace_layout,
                volume_layout,
            ))),
        }
    }

    pub fn get_engine(&mut self, engine_id: &str) -> Result<Arc<Mutex<dyn Engine>>, EngineError> {
        match engine_id {
            "sparkSQL" => Ok(self.spark_engine.clone()),
            _ => Err(EngineError::not_found(engine_id)),
        }
    }
}
