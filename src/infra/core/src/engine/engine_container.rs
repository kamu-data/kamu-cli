// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use container_runtime::*;
use kamu_core::engine::EngineError;
use odf::metadata::engine::EngineGrpcClient;

use super::ODFEngineConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EngineContainer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct EngineContainer {
    container: ContainerProcess,
    host: String,
    port: u16,
    logs_config: LogsConfig,
}

impl EngineContainer {
    const ADAPTER_PORT: u16 = 2884;

    #[tracing::instrument(level = "info", name = "init_engine", skip_all, fields(%image))]
    pub async fn new(
        container_runtime: Arc<ContainerRuntime>,
        engine_config: ODFEngineConfig,
        logs_config: LogsConfig,
        image: &str,
        volumes: Vec<VolumeSpec>,
        operation_id: &str,
    ) -> Result<Self, EngineError> {
        let stdout_file = std::fs::File::create(&logs_config.stdout_path)?;
        let stderr_file = std::fs::File::create(&logs_config.stderr_path)?;

        let container = container_runtime
            .run_attached(image)
            .container_name(format!("kamu-engine-{operation_id}"))
            .volumes(volumes)
            .expose_port(Self::ADAPTER_PORT)
            .stdout(stdout_file)
            .stderr(stderr_file)
            .terminate_timeout(engine_config.shutdown_timeout)
            .spawn()
            .map_err(|e| EngineError::internal(e, logs_config.log_files()))?;

        let adapter_host_port = container
            .wait_for_host_socket(Self::ADAPTER_PORT, engine_config.start_timeout)
            .await
            .map_err(|e| EngineError::internal(e, logs_config.log_files()))?;

        Ok(Self {
            container,
            host: container_runtime.get_runtime_host_addr(),
            port: adapter_host_port,
            logs_config,
        })
    }

    pub fn container_name(&self) -> &str {
        self.container.container_name()
    }

    pub async fn connect_client(&self) -> Result<EngineGrpcClient, EngineError> {
        EngineGrpcClient::connect(&self.host, self.port)
            .await
            .map_err(|e| EngineError::internal(e, self.logs_config.log_files()))
    }

    pub async fn terminate(mut self) -> std::io::Result<TerminateStatus> {
        self.container.terminate().await
    }

    pub fn log_files(&self) -> Vec<PathBuf> {
        self.logs_config.log_files()
    }
}

impl std::ops::Deref for EngineContainer {
    type Target = ContainerProcess;

    fn deref(&self) -> &Self::Target {
        &self.container
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// LogsConfig
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct LogsConfig {
    logs_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl LogsConfig {
    pub fn new(logs_dir: &Path) -> Self {
        Self {
            stdout_path: logs_dir.join("engine.stdout.txt"),
            stderr_path: logs_dir.join("engine.stderr.txt"),
            logs_dir: logs_dir.to_path_buf(),
        }
    }

    fn log_files(&self) -> Vec<PathBuf> {
        let mut logs = self.demux_logs().unwrap_or_default();
        logs.push(self.stdout_path.clone());
        logs.push(self.stderr_path.clone());
        logs
    }

    // ODF adapters log in bunyan format (JSON per line)
    // To make logs more readable we parse the logs to demultiplex
    // logs from multiple processes into different files
    fn demux_logs(&self) -> Result<Vec<PathBuf>, std::io::Error> {
        use std::collections::BTreeMap;
        use std::fs::File;
        use std::io::{BufRead, Write};

        let mut demuxed: BTreeMap<String, (PathBuf, File)> = BTreeMap::new();
        let file = File::open(&self.stdout_path)?;
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let obj: serde_json::Value =
                serde_json::from_str(&line).unwrap_or(serde_json::Value::Null);
            let process = &obj["process"];
            let stream = &obj["stream"];
            if process.is_null() || !process.is_string() || stream.is_null() || !stream.is_string()
            {
                continue;
            }

            let filename =
                self.demuxed_filename(process.as_str().unwrap(), stream.as_str().unwrap());

            let file = match demuxed.get_mut(&filename) {
                Some((_, f)) => f,
                None => {
                    let path = self.logs_dir.join(&filename);
                    let f = File::create(&path)?;
                    demuxed.insert(filename.clone(), (path, f));
                    &mut demuxed.get_mut(&filename).unwrap().1
                }
            };

            writeln!(file, "{}", obj["msg"].as_str().unwrap_or_default())?;
        }

        Ok(demuxed.into_values().map(|(path, _)| path).collect())
    }

    fn demuxed_filename(&self, process: &str, stream: &str) -> String {
        format!("engine-{process}.{stream}.txt")
    }
}
