use crate::domain::PullImageListener;
use crate::infra::utils::docker_client::*;
use crate::infra::utils::docker_images;
use crate::infra::*;

use slog::{info, Logger};
use std::fs::File;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct LivyServerImpl {
    container_runtime: Arc<DockerClient>,
}

impl LivyServerImpl {
    pub fn new(container_runtime: Arc<DockerClient>) -> Self {
        Self { container_runtime }
    }

    pub fn ensure_images(&self, listener: &mut dyn PullImageListener) {
        self.container_runtime
            .ensure_image(docker_images::LIVY, Some(listener));
    }

    pub fn run<StartedClb>(
        &self,
        addr: &str,
        host_port: u16,
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        inherit_stdio: bool,
        on_started: StartedClb,
        logger: Logger,
    ) -> Result<(), std::io::Error>
    where
        StartedClb: FnOnce() + Send + 'static,
    {
        const LIVY_PORT: u16 = 8998;

        let livy_stdout_path = workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = workspace_layout.run_info_dir.join("livy.err.txt");

        let mut livy_cmd = self.container_runtime.run_cmd(DockerRunArgs {
            image: docker_images::LIVY.to_owned(),
            container_name: Some("kamu-livy".to_owned()),
            args: vec!["livy".to_owned()],
            user: Some("root".to_owned()),
            expose_port_map_addr: vec![(addr.to_owned(), host_port, LIVY_PORT)],
            volume_map: if volume_layout.data_dir.exists() {
                vec![(
                    volume_layout.data_dir.clone(),
                    PathBuf::from("/opt/spark/work-dir"),
                )]
            } else {
                vec![]
            },
            ..DockerRunArgs::default()
        });

        info!(logger, "Starting Livy container"; "command" => ?livy_cmd);

        let mut livy = livy_cmd
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stdout_path)?)
            })
            .stderr(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stderr_path)?)
            })
            .spawn()?;

        let _drop_livy = DropContainer::new(self.container_runtime.clone(), "kamu-livy");

        self.container_runtime
            .wait_for_socket(host_port, Duration::from_secs(60))
            .expect("Livy did not start");

        on_started();

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone())?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone())?;

        while !exit.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        if !cfg!(windows) {
            unsafe {
                libc::kill(livy.id() as libc::pid_t, libc::SIGTERM);
            }
        }

        livy.wait()?;
        Ok(())
    }
}
