use crate::infra::utils::docker_client::*;
use crate::infra::*;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const LIVY_IMAGE: &str = "kamudata/engine-spark:0.8.0";
const JUPYTER_IMAGE: &str = "kamudata/jupyter-uber:0.0.1";

pub struct NotebookServerImpl;

impl NotebookServerImpl {
    pub fn run<StartedClb, ShutdownClb>(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        environment_vars: Vec<(String, String)>,
        inherit_stdio: bool,
        on_started: StartedClb,
        on_shutdown: ShutdownClb,
    ) -> Result<(), std::io::Error>
    where
        StartedClb: FnOnce(&str) + Send + 'static,
        ShutdownClb: FnOnce() + Send + 'static,
    {
        let docker_client = DockerClient::new();

        let network_name = "kamu";
        let _network = docker_client.create_network(network_name);

        let cwd = Path::new(".").canonicalize().unwrap();

        let livy_stdout_path = workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = workspace_layout.run_info_dir.join("livy.err.txt");
        let jupyter_stdout_path = workspace_layout.run_info_dir.join("jupyter.out.txt");
        let jupyter_stderr_path = workspace_layout.run_info_dir.join("jupyter.err.txt");

        let mut livy = docker_client
            .run_cmd(DockerRunArgs {
                image: LIVY_IMAGE.to_owned(),
                container_name: Some("kamu-livy".to_owned()),
                hostname: Some("kamu-livy".to_owned()),
                network: Some(network_name.to_owned()),
                args: vec!["livy".to_owned()],
                volume_map: if volume_layout.data_dir.exists() {
                    vec![(
                        volume_layout.data_dir.clone(),
                        PathBuf::from("/opt/spark/work-dir"),
                    )]
                } else {
                    vec![]
                },
                interactive: true,
                ..DockerRunArgs::default()
            })
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
        let _drop_livy = DropContainer::new(docker_client.clone(), "kamu-livy");

        // TODO: env vars propagation
        let mut jupyter = docker_client
            .run_cmd(DockerRunArgs {
                image: JUPYTER_IMAGE.to_owned(),
                container_name: Some("kamu-jupyter".to_owned()),
                network: Some(network_name.to_owned()),
                expose_ports: vec![80],
                volume_map: vec![(cwd.clone(), PathBuf::from("/opt/workdir"))],
                interactive: true,
                environment_vars: environment_vars,
                ..DockerRunArgs::default()
            })
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&jupyter_stdout_path)?)
            })
            .stderr(Stdio::piped())
            .spawn()?;
        let _drop_jupyter = DropContainer::new(docker_client.clone(), "kamu-jupyter");
        let jupyter_port = docker_client
            .wait_for_host_port("kamu-jupyter", 80, std::time::Duration::from_secs(5))
            .unwrap_or_default();

        let token_clb = move |token: &str| {
            let url = format!("http://localhost:{}/?token={}", jupyter_port, token);
            on_started(&url);
        };

        let token_extractor = if inherit_stdio {
            TokenExtractor::new(
                jupyter.stderr.take().unwrap(),
                std::io::stderr(),
                Some(token_clb),
            )?
        } else {
            TokenExtractor::new(
                jupyter.stderr.take().unwrap(),
                File::create(&jupyter_stderr_path)?,
                Some(token_clb),
            )?
        };

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(signal_hook::SIGINT, exit.clone())?;
        signal_hook::flag::register(signal_hook::SIGTERM, exit.clone())?;

        // Relying on shell to send signal to child processes
        while !exit.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        on_shutdown();

        jupyter.wait()?;
        livy.wait()?;
        token_extractor.handle.join().unwrap();

        // Fix permissions
        if cfg!(unix) {
            docker_client
                .run_shell_cmd(
                    DockerRunArgs {
                        image: JUPYTER_IMAGE.to_owned(),
                        container_name: Some("kamu-jupyter".to_owned()),
                        volume_map: vec![(cwd, PathBuf::from("/opt/workdir"))],
                        ..DockerRunArgs::default()
                    },
                    &[format!(
                        "chown -R {}:{} {}",
                        users::get_current_uid(),
                        users::get_current_gid(),
                        "/opt/workdir"
                    )],
                )
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////
// TokenExtractor
///////////////////////////////////////////////////////////////////////////////

struct TokenExtractor {
    handle: std::thread::JoinHandle<()>,
}

impl TokenExtractor {
    fn new<R, W, Clb>(
        input: R,
        mut output: W,
        mut on_token: Option<Clb>,
    ) -> Result<Self, std::io::Error>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Write + Send + 'static,
        Clb: FnOnce(&str) + Send + 'static,
    {
        let handle = std::thread::Builder::new()
            .name("jupyter-io".to_owned())
            .spawn({
                move || {
                    use std::io::BufRead;
                    let mut reader = std::io::BufReader::new(input);
                    let mut line = String::with_capacity(128);
                    let re = regex::Regex::new("token=([a-z0-9]+)").unwrap();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).unwrap() == 0 {
                            break;
                        }
                        output.write_all(line.as_bytes()).unwrap();
                        if let Some(capture) = re.captures(&line) {
                            if let Some(clb) = on_token.take() {
                                let token = capture.get(1).unwrap().as_str();
                                clb(token);
                            }
                        }
                    }
                }
            })?;
        Ok(Self { handle: handle })
    }
}
