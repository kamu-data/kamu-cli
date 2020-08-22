use std::path::{Path, PathBuf};

/// Describes the layout of the workspace on disk
#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    /// Root directory of all metadata and configuration
    pub kamu_root_dir: PathBuf,
    /// Contains dataset metadata
    pub datasets_dir: PathBuf,
    /// Contains remote definitions
    pub remotes_dir: PathBuf,
    /// Directory for storing per-run diagnostics information and logs
    pub run_info_dir: PathBuf,
    /// Root directory of a local storage volume
    pub local_volume_dir: PathBuf,
}

impl WorkspaceLayout {
    pub fn new(workspace_root: &Path) -> Self {
        let kamu_root_dir = workspace_root.join(".kamu");
        Self {
            datasets_dir: kamu_root_dir.join("datasets"),
            remotes_dir: kamu_root_dir.join("remotes"),
            run_info_dir: kamu_root_dir.join("run"),
            kamu_root_dir: kamu_root_dir,
            local_volume_dir: workspace_root.join(".kamu.local"),
        }
    }

    pub fn create(workspace_root: &Path) -> Result<Self, std::io::Error> {
        let ws = Self::new(workspace_root);
        std::fs::create_dir_all(&ws.kamu_root_dir)?;
        std::fs::create_dir_all(&ws.datasets_dir)?;
        std::fs::create_dir_all(&ws.remotes_dir)?;
        std::fs::create_dir_all(&ws.run_info_dir)?;
        std::fs::create_dir_all(&ws.local_volume_dir)?;
        Ok(ws)
    }
}
