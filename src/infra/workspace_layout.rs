use std::path::PathBuf;

/// Describes the layout of the workspace on disk
#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    /// Root directory of all metadata and configuration
    pub kamu_root_dir: PathBuf,
    /// Contains dataset metadata
    pub datasets_dir: PathBuf,
    /// Contains remote definitions
    pub remotes_dir: PathBuf,
    /// Root directory of a local storage volume
    pub local_volume_dir: PathBuf,
}
