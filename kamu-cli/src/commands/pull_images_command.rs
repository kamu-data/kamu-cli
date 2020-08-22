use super::{Command, Error};
use kamu::infra::utils::docker_client::DockerClient;
use kamu::infra::utils::docker_images;

pub struct PullImagesCommand {}

impl PullImagesCommand {
    pub fn new<'a>() -> Self {
        Self {}
    }
}

impl Command for PullImagesCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let docker_client = DockerClient::new();

        let images = [
            docker_images::SPARK,
            docker_images::SPARK_V3,
            docker_images::FLINK,
            docker_images::JUPYTER,
        ];

        for img in images.iter() {
            eprintln!("{}: {}", console::style("Pulling image").bold(), img);
            docker_client.pull_cmd(img).status()?;
        }

        Ok(())
    }
}
