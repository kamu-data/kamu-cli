use std::path::PathBuf;

/// Describes the layout of the workspace on disk
#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    /// Contains dataset metadata
    pub metadata_dir: PathBuf,
    /// Contains remote definitions
    pub remotes_dir: PathBuf,
    /// Root directory of a local storage volume
    pub local_volume_dir: PathBuf,
}
