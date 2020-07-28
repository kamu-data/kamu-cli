use std::path::{Path, PathBuf};

/// Describes the layout of the data volume on disk
#[derive(Debug, Clone)]
pub struct VolumeLayout {
    /// Directory that contains metadata of all datasets contained in this volume
    pub metadata_dir: PathBuf,
    /// Directory that contains processing checkpoints
    pub checkpoints_dir: PathBuf,
    /// Directory that stores the actual data
    pub data_dir: PathBuf,
    /// Stores data that is not essential but can improve performance of operations like data polling
    pub cache_dir: PathBuf,
}

impl VolumeLayout {
    pub fn new(volume_root: &Path) -> Self {
        Self {
            metadata_dir: volume_root.join("datasets"),
            checkpoints_dir: volume_root.join("checkpoints"),
            data_dir: volume_root.join("data"),
            cache_dir: volume_root.join("cache"),
        }
    }

    pub fn create(volume_root: &Path) -> Result<Self, std::io::Error> {
        let vol = Self::new(volume_root);
        std::fs::create_dir_all(&vol.metadata_dir)?;
        std::fs::create_dir_all(&vol.checkpoints_dir)?;
        std::fs::create_dir_all(&vol.data_dir)?;
        std::fs::create_dir_all(&vol.cache_dir)?;
        Ok(vol)
    }
}
