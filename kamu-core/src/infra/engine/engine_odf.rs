use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::Arc,
    time::Duration,
};

use opendatafabric as odf;
use rand::Rng;
use slog::{info, Logger};

use crate::domain::*;
use crate::infra::{
    utils::docker_client::{DockerClient, DockerRunArgs},
    WorkspaceLayout,
};

pub struct ODFEngine {
    container_runtime: DockerClient,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
    logger: Logger,
}

impl ODFEngine {
    const ADAPTER_PORT: u16 = 2884;
    const START_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn new(
        container_runtime: DockerClient,
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

    fn transform2(
        &self,
        run_info: RunInfo,
        _request: odf::ExecuteQueryRequest,
    ) -> Result<odf::ExecuteQueryResponseSuccess, EngineError> {
        let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

        let container_name = format!("kamu-engine-{}", run_info.run_id);

        let mut cmd = self.container_runtime.run_cmd(DockerRunArgs {
            image: self.image.clone(),
            container_name: Some(container_name.clone()),
            //volume_map: volume_map,
            user: Some("root".to_owned()),
            expose_ports: vec![Self::ADAPTER_PORT],
            ..DockerRunArgs::default()
        });

        info!(self.logger, "Starting engine"; "command" => ?cmd, "image" => &self.image);

        let _engine_process = OwnedProcess(
            cmd.stdout(Stdio::inherit()) //std::process::Stdio::from(stdout_file))
                .stderr(Stdio::inherit()) //std::process::Stdio::from(stderr_file))
                .spawn()
                .map_err(|e| EngineError::internal(e))?,
        );

        let _adapter_host_port = self
            .container_runtime
            .wait_for_host_port(&container_name, Self::ADAPTER_PORT, Self::START_TIMEOUT)
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

        let response = self.transform2(run_info, request)?;

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

struct OwnedProcess(Child);

impl OwnedProcess {
    pub fn has_exited(&mut self) -> Result<bool, std::io::Error> {
        Ok(self.0.try_wait()?.map(|_| true).unwrap_or(false))
    }
}

impl Deref for OwnedProcess {
    type Target = Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OwnedProcess {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }

        let start = chrono::Utc::now();

        while (chrono::Utc::now() - start).num_seconds() < 3 {
            if self.has_exited().unwrap_or(true) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = self.0.kill();
    }
}
