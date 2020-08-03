use std::path::PathBuf;
use std::process::Command;

pub struct DockerRunArgs {
    pub image: String,
    pub args: Vec<String>,
    pub container_name: Option<String>,
    pub hostname: Option<String>,
    pub network: Option<String>,
    pub expose_all_ports: bool,
    pub expose_ports: Vec<u16>,
    pub expose_port_map: Vec<(u16, u16)>,
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
        args.volume_map.iter().for_each(|(h, c)| {
            cmd.arg("-v");
            cmd.arg(format!("{}:{}", h.display(), c.display()));
        });
        args.work_dir
            .map(|v| cmd.arg(format!("--workdir={}", v.display())));
        args.environment_vars.iter().for_each(|(n, v)| {
            cmd.arg("-e");
            cmd.arg(format!("{}:{}", n, v));
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
}
