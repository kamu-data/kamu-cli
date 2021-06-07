use kamu::infra::utils::docker_client::*;
use kamu::infra::utils::docker_images;

use std::path::{Path, PathBuf};
use std::time::Duration;

// TODO: Consider replacing with in-process server for speed
pub struct HttpServer {
    container_runtime: DockerClient,
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    process: std::process::Child,
}

impl HttpServer {
    pub fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let container_runtime = DockerClient::default();

        let mut server_name = "kamu-test-http-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30)
                .map(char::from),
        );
        let server_port = 80;

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        assert!(
            container_runtime.has_image(docker_images::HTTPD),
            "Please pull {} image before running this test",
            docker_images::HTTPD
        );

        let process = container_runtime
            .run_cmd(DockerRunArgs {
                image: docker_images::HTTPD.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![server_port],
                volume_map: vec![(
                    server_dir.to_owned(),
                    PathBuf::from("/usr/local/apache2/htdocs"),
                )],
                ..DockerRunArgs::default()
            })
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container_runtime
            .wait_for_host_port(&server_name, server_port, Duration::from_secs(20))
            .unwrap();

        container_runtime
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        let address = container_runtime.get_docker_addr();

        Self {
            container_runtime,
            container_name: server_name,
            process: process,
            address: address,
            host_port: host_port,
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let _ = self
            .container_runtime
            .kill_cmd(&self.container_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.wait();
    }
}
