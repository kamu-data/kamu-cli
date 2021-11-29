use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ODFEngineConfig {
    pub start_timeout: Duration,
    pub shutdown_timeout: Duration,
}
