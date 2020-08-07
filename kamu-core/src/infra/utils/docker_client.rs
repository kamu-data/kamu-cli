use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use std::backtrace::Backtrace;
use thiserror::Error;

pub struct DockerRunArgs {
    pub image: String,
    pub args: Vec<String>,
    pub container_name: Option<String>,
    pub hostname: Option<String>,
    pub network: Option<String>,
    pub expose_all_ports: bool,
    pub expose_ports: Vec<u16>,
    pub expose_port_map: Vec<(u16, u16)>,
    pub expose_port_map_range: Vec<((u16, u16), (u16, u16))>,
    pub volume_map: Vec<(PathBuf, PathBuf)>,
    pub work_dir: Option<PathBuf>,
    pub environment_vars: Vec<(String, String)>,
    pub entry_point: Option<String>,
    pub remove: bool,
    pub tty: bool,
    pub interactive: bool,
    pub detached: bool,
}

pub struct ExecArgs {
    pub tty: bool,
    pub interactive: bool,
}

impl Default for DockerRunArgs {
    fn default() -> Self {
        Self {
            image: "".to_owned(),
            args: Vec::new(),
            container_name: None,
            hostname: None,
            network: None,
            expose_all_ports: false,
            expose_ports: Vec::new(),
            expose_port_map: Vec::new(),
            expose_port_map_range: Vec::new(),
            volume_map: Vec::new(),
            work_dir: None,
            environment_vars: Vec::new(),
            entry_point: None,
            remove: true,
            tty: false,
            interactive: false,
            detached: false,
        }
    }
}

pub struct DockerClient;

impl DockerClient {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run_cmd(&self, args: DockerRunArgs) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("run");
        if args.remove {
            cmd.arg("--rm");
        }
        if args.tty {
            cmd.arg("-t");
        }
        if args.interactive {
            cmd.arg("-i");
        }
        if args.detached {
            cmd.arg("-d");
        }
        args.container_name
            .map(|v| cmd.arg(format!("--name={}", v)));
        args.hostname.map(|v| cmd.arg(format!("--hostname={}", v)));
        args.network.map(|v| cmd.arg(format!("--network={}", v)));
        if args.expose_all_ports {
            cmd.arg("-P");
        }
        args.expose_ports.iter().for_each(|v| {
            cmd.arg("-p");
            cmd.arg(format!("{}", v));
        });
        args.expose_port_map.iter().for_each(|(h, c)| {
            cmd.arg("-p");
            cmd.arg(format!("{}:{}", h, c));
        });
        args.expose_port_map_range
            .iter()
            .for_each(|((hl, hr), (cl, cr))| {
                cmd.arg("-p");
                cmd.arg(format!("{}-{}:{}-{}", hl, hr, cl, cr));
            });
        args.volume_map.iter().for_each(|(h, c)| {
            cmd.arg("-v");
            cmd.arg(format!("{}:{}", h.display(), c.display()));
        });
        args.work_dir
            .map(|v| cmd.arg(format!("--workdir={}", v.display())));
        args.environment_vars.iter().for_each(|(n, v)| {
            cmd.arg("-e");
            cmd.arg(format!("{}={}", n, v));
        });
        args.entry_point
            .map(|v| cmd.arg(format!("--entrypoint={}", v)));
        cmd.arg(args.image);
        cmd.args(args.args);
        cmd
    }

    pub fn run_shell_cmd<I, S>(&self, args: DockerRunArgs, shell_cmd: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let shell_cmd_vec: Vec<String> = shell_cmd
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        self.run_cmd(DockerRunArgs {
            entry_point: Some("bash".to_owned()),
            args: vec!["-c".to_owned(), shell_cmd_vec.join(" ")],
            ..args
        })
    }

    pub fn kill_cmd(&self, container_name: &str) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("kill").arg(container_name);
        cmd
    }

    pub fn get_host_port(&self, container_name: &str, container_port: u16) -> Option<u16> {
        let format = format!(
            "--format={{{{ (index (index .NetworkSettings.Ports \"{}/tcp\") 0).HostPort }}}}",
            container_port
        );

        //let formatEscaped =
        //  if (!OS.isWindows) format else format.replace("\"", "\\\"")

        let res = Command::new("docker")
            .arg("inspect")
            .arg(format)
            .arg(container_name)
            .output();

        match res {
            Ok(output) => std::str::from_utf8(&output.stdout)
                .unwrap()
                .trim_matches(&['\r', '\n'][..])
                .parse()
                .ok(),
            _ => None,
        }
    }

    pub fn wait_for_host_port(
        &self,
        container_name: &str,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, TimeoutError> {
        let start = Instant::now();
        loop {
            let res = self.get_host_port(container_name, container_port);
            if let Some(hp) = res {
                break Ok(hp);
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout));
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }

    pub fn check_socket(&self, host_port: u16) -> bool {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), host_port);
        TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok()
    }

    pub fn wait_for_socket(&self, host_port: u16, timeout: Duration) -> Result<(), TimeoutError> {
        let start = Instant::now();
        loop {
            if self.check_socket(host_port) {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout));
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

#[derive(Error, Debug)]
#[error("Timed out after {duration:?}")]
pub struct TimeoutError {
    duration: Duration,
    backtrace: Backtrace,
}
impl TimeoutError {
    pub fn new(d: Duration) -> Self {
        Self {
            duration: d,
            backtrace: Backtrace::capture(),
        }
    }
}
