use kamu::infra::utils::docker_client::*;
use kamu::infra::utils::docker_images;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

// TODO: Consider replacing with in-process server for speed
pub struct HttpServer {
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    process: std::process::Child,
}

impl HttpServer {
    pub fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let docker = DockerClient::new();

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

        let image = docker_images::HTTPD;

        docker
            .pull_cmd(image)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let process = docker
            .run_cmd(DockerRunArgs {
                image: image.to_owned(),
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

        let host_port = docker
            .wait_for_host_port(&server_name, server_port, Duration::from_secs(20))
            .unwrap();

        docker
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        Self {
            container_name: server_name,
            process: process,
            address: docker.get_docker_addr(),
            host_port: host_port,
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let docker = DockerClient::new();
        let _ = docker
            .kill_cmd(&self.container_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.wait();
    }
}
