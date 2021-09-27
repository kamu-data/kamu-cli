use std::{
    path::{Path, PathBuf},
    process::Child,
    sync::Arc,
    time::Duration,
};

use container_runtime::{ContainerRuntime, ContainerRuntimeType, ExecArgs, RunArgs};
use odf::{
    engine::{EngineClient, ExecuteQueryError},
    ExecuteQueryRequest, ExecuteQueryResponseSuccess, QueryInput,
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
    const CT_VOLUME_DIR: &'static str = "/opt/engine/volume";

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
            &run_info,
            vec![(
                self.workspace_layout.local_volume_dir.clone(),
                PathBuf::from(Self::CT_VOLUME_DIR),
            )],
            self.logger.clone(),
        )?;

        let mut client = engine_container.connect_client().await?;
        let response = client.execute_query(request).await;

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
                    self.container_runtime.exec_shell_cmd(ExecArgs::default(), &engine_container.container_name, &[format!(
                        "chown -R {}:{} {}",
                        users::get_current_uid(),
                        users::get_current_gid(),
                        Self::CT_VOLUME_DIR
                    )]).status()?;
                }
            }
        }

        response.map_err(|e| e.into())
    }

    fn to_container_path(&self, host_path: &Path) -> PathBuf {
        let host_path = Self::canonicalize_via_parent(host_path).unwrap();
        let volume_path = self
            .workspace_layout
            .local_volume_dir
            .canonicalize()
            .unwrap();
        let volume_rel_path = host_path.strip_prefix(volume_path).unwrap();

        let mut container_path = Self::CT_VOLUME_DIR.to_owned();
        container_path.push('/');
        container_path.push_str(&volume_rel_path.to_string_lossy());
        PathBuf::from(container_path)
    }

    fn canonicalize_via_parent(path: &Path) -> Result<PathBuf, std::io::Error> {
        match path.canonicalize() {
            Ok(p) => Ok(p),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    let mut cp = Self::canonicalize_via_parent(parent)?;
                    cp.push(path.file_name().unwrap());
                    Ok(cp)
                } else {
                    Err(e)
                }
            }
            e @ _ => e,
        }
    }
}

impl Engine for ODFEngine {
    fn transform(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let request_adj = ExecuteQueryRequest {
            prev_checkpoint_dir: request
                .prev_checkpoint_dir
                .map(|p| self.to_container_path(&p)),
            new_checkpoint_dir: self.to_container_path(&request.new_checkpoint_dir),
            out_data_path: self.to_container_path(&request.out_data_path),
            inputs: request
                .inputs
                .into_iter()
                .map(|input| QueryInput {
                    data_paths: input
                        .data_paths
                        .into_iter()
                        .map(|p| self.to_container_path(&p))
                        .collect(),
                    schema_file: self.to_container_path(&input.schema_file),
                    ..input
                })
                .collect(),
            ..request
        };

        let run_info = RunInfo::new(&self.workspace_layout.run_info_dir);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let response = rt.block_on(self.transform2(run_info, request_adj))?;
        Ok(response)
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
        run_info: &RunInfo,
        volume_map: Vec<(PathBuf, PathBuf)>,
        logger: Logger,
    ) -> Result<Self, EngineError> {
        let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

        let container_name = format!("kamu-engine-{}", &run_info.run_id);

        let mut cmd = container_runtime.run_cmd(RunArgs {
            image: image.to_owned(),
            container_name: Some(container_name.clone()),
            volume_map: volume_map,
            user: Some("root".to_owned()),
            expose_ports: vec![Self::ADAPTER_PORT],
            ..RunArgs::default()
        });

        info!(logger, "Starting engine"; "command" => ?cmd, "image" => image, "id" => &container_name);

        let engine_process = cmd
            .stdout(std::process::Stdio::from(stdout_file)) // Stdio::inherit()
            .stderr(std::process::Stdio::from(stderr_file)) // Stdio::inherit()
            .spawn()
            .map_err(|e| EngineError::internal(e))?;

        let adapter_host_port = container_runtime
            .wait_for_host_port(&container_name, Self::ADAPTER_PORT, Self::START_TIMEOUT)
            .map_err(|e| EngineError::internal(e))?;

        container_runtime
            .wait_for_socket(adapter_host_port, Self::START_TIMEOUT)
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

    pub fn has_exited(&mut self) -> bool {
        match self.engine_process.try_wait() {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => true,
        }
    }
}

impl Drop for EngineContainer {
    fn drop(&mut self) {
        if self.has_exited() {
            return;
        }

        info!(self.logger, "Shutting down engine"; "id" => &self.container_name);
        unsafe {
            libc::kill(self.engine_process.id() as i32, libc::SIGTERM);
        }

        let start = std::time::Instant::now();
        while (std::time::Instant::now() - start) < Self::SHUTDOWN_TIMEOUT {
            if self.has_exited() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        warn!(self.logger, "Engine did not shutdown gracefully, killing"; "id" => &self.container_name);
        let _ = self.engine_process.kill();
    }
}

///////////////////////////////////////////////////////////////////////////////

impl From<ExecuteQueryError> for EngineError {
    fn from(e: ExecuteQueryError) -> Self {
        match e {
            ExecuteQueryError::InvalidQuery(e) => EngineError::InvalidQuery { message: e.message },
            e @ ExecuteQueryError::InternalError(_) => EngineError::internal(e),
            e @ ExecuteQueryError::RpcError(_) => EngineError::internal(e),
        }
    }
}
