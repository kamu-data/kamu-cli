use std::{
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::Arc,
    time::Duration,
};

use container_runtime::{ContainerRuntime, RunArgs};
use odf::{
    engine::EngineClient,
    serde::{flatbuffers::FlatbuffersEngineProtocol, EngineProtocolSerializer},
};
use opendatafabric as odf;
use rand::Rng;
use slog::{info, warn, Logger};

use crate::domain::*;
use crate::infra::WorkspaceLayout;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
    logger: Logger,
}

impl ODFEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
        image: &str,
        workspace_layout: Arc<WorkspaceLayout>,
        logger: Logger,
    ) -> Self {
        Self {
            container_runtime,
            image: image.to_owned(),
            workspace_layout,
            logger,
        }
    }

    async fn transform2(
        &self,
        run_info: RunInfo,
        request: odf::ExecuteQueryRequest,
    ) -> Result<odf::ExecuteQueryResponseSuccess, EngineError> {
        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            &self.image,
            &run_info.run_id,
            self.logger.clone(),
        )?;

        let mut client = engine_container.connect_client().await?;

        client
            .execute_query(request)
            .await
            .map_err(|e| EngineError::internal(e))?;

        // TODO: chown
        unimplemented!()
    }
}

impl Engine for ODFEngine {
    fn transform(
        &self,
        mut request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, EngineError> {
        let mut inputs = Vec::new();
        for input_id in request.source.inputs {
            let slice = request.input_slices.remove(&input_id).unwrap();
            let vocab = request.dataset_vocabs.remove(&input_id).unwrap();
            inputs.push(odf::QueryInput {
                dataset_id: input_id,
                slice: odf::InputDataSlice {
                    interval: slice.interval,
                    schema_file: slice.schema_file.to_string_lossy().to_string(),
                    explicit_watermarks: slice.explicit_watermarks,
                },
                vocab: vocab,
            });
        }

        let request = odf::ExecuteQueryRequest {
            dataset_id: request.dataset_id,
            transform: request.source.transform,
            inputs: inputs,
        };

        let run_info = RunInfo::new(&self.workspace_layout.run_info_dir);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let response = rt.block_on(self.transform2(run_info, request))?;

        Ok(ExecuteQueryResponse {
            block: response.metadata_block,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct RunInfo {
    run_id: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl RunInfo {
    fn new(logs_dir: &Path) -> Self {
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let stdout_path = logs_dir.join(format!("engine-{}.out.txt", &run_id));
        let stderr_path = logs_dir.join(format!("engine-{}.err.txt", &run_id));

        Self {
            run_id,
            stdout_path,
            stderr_path,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct EngineContainer {
    container_runtime: ContainerRuntime,
    container_name: String,
    adapter_host_port: u16,
    engine_process: Child,
    logger: Logger,
}

impl EngineContainer {
    const ADAPTER_PORT: u16 = 2884;
    const START_TIMEOUT: Duration = Duration::from_secs(30);
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

    pub fn new(
        container_runtime: ContainerRuntime,
        image: &str,
        run_id: &str,
        logger: Logger,
    ) -> Result<Self, EngineError> {
        //let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        //let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

        let container_name = format!("kamu-engine-{}", run_id);

        let mut cmd = container_runtime.run_cmd(RunArgs {
            image: image.to_owned(),
            container_name: Some(container_name.clone()),
            //volume_map: volume_map,
            user: Some("root".to_owned()),
            expose_ports: vec![Self::ADAPTER_PORT],
            ..RunArgs::default()
        });

        info!(logger, "Starting engine"; "command" => ?cmd, "image" => image, "id" => &container_name);

        let engine_process = cmd
            .stdout(Stdio::inherit()) //std::process::Stdio::from(stdout_file))
            .stderr(Stdio::inherit()) //std::process::Stdio::from(stderr_file))
            .spawn()
            .map_err(|e| EngineError::internal(e))?;

        let adapter_host_port = container_runtime
            .wait_for_host_port(&container_name, Self::ADAPTER_PORT, Self::START_TIMEOUT)
            .map_err(|e| EngineError::internal(e))?;

        info!(logger, "Engine running"; "id" => &container_name);

        Ok(Self {
            container_runtime,
            container_name,
            adapter_host_port,
            engine_process,
            logger,
        })
    }

    pub async fn connect_client(&self) -> Result<EngineClient, EngineError> {
        Ok(EngineClient::connect(
            &self.container_runtime.get_runtime_host_addr(),
            self.adapter_host_port,
        )
        .await
        .map_err(|e| EngineError::internal(e))?)
    }

    pub fn has_exited(&mut self) -> Result<bool, std::io::Error> {
        Ok(self
            .engine_process
            .try_wait()?
            .map(|_| true)
            .unwrap_or(false))
    }
}

impl Drop for EngineContainer {
    fn drop(&mut self) {
        info!(self.logger, "Shutting down engine"; "id" => &self.container_name);
        unsafe {
            libc::kill(self.engine_process.id() as i32, libc::SIGTERM);
        }

        let start = std::time::Instant::now();
        while (std::time::Instant::now() - start) < Self::SHUTDOWN_TIMEOUT {
            if self.has_exited().unwrap_or(true) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        warn!(self.logger, "Engine did not shutdown gracefully, killing"; "id" => &self.container_name);
        let _ = self.engine_process.kill();
    }
}
