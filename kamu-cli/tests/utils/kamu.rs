use std::ffi::{OsStr, OsString};
use std::fmt::Display;
use std::io::Write;
use std::path::{Path, PathBuf};

use kamu::infra::*;
use kamu_cli::CLIError;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

// Test wrapper on top of CLI library
pub struct Kamu {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    workspace_path: PathBuf,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Kamu {
    pub fn new(workspace_path: &Path) -> Self {
        let workspace_layout = WorkspaceLayout::new(workspace_path);
        let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
        Self {
            workspace_layout,
            volume_layout,
            workspace_path: workspace_path.to_owned(),
            _temp_dir: None,
        }
    }

    pub fn new_workspace_tmp() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let inst = Self::new(temp_dir.path());
        let inst = Self {
            _temp_dir: Some(temp_dir),
            ..inst
        };
        inst.execute(["init"]).unwrap();
        inst
    }

    pub fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    pub fn execute<I, S>(&self, cmd: I) -> Result<(), CLIError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut full_cmd = vec![OsStr::new("kamu").to_owned()];
        full_cmd.extend(cmd.into_iter().map(|i| i.as_ref().to_owned()));

        let app = kamu_cli::cli();
        let matches = app.get_matches_from_safe(full_cmd).unwrap();

        kamu_cli::run(
            self.workspace_layout.clone(),
            self.volume_layout.clone(),
            matches,
        )
    }

    pub fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) -> Result<(), CLIError> {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()])
    }
}

#[derive(Debug)]
pub struct CommandError {
    cmd: Vec<OsString>,
    code: i32,
    stdout: String,
    stderr: String,
}

impl std::error::Error for CommandError {}

impl Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Command execution failed")?;
        write!(f, "Cmd: ")?;
        for s in &self.cmd {
            write!(f, "\"{:?}\", ", s)?;
        }
        writeln!(f, "")?;
        writeln!(f, "Exit code: {}", self.code)?;
        writeln!(f, "Stdout: {}", self.stdout)?;
        writeln!(f, "Stderr: {}", self.stderr)?;
        Ok(())
    }
}
