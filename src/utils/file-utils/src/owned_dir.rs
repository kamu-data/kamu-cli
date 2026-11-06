// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A working directory on local FS that is removed with all its contents on
/// drop, unless released with [`OwnedDir::keep`]. A directory that is already
/// gone is fine; any other failure to remove it is logged as a warning naming
/// the directory's `description`, since removal is best effort.
#[derive(Debug)]
pub struct OwnedDir {
    path: Option<PathBuf>,
    description: &'static str,
}

impl OwnedDir {
    /// `description` names the directory in the warning, e.g. "engine run
    /// directory"
    pub fn new(path: impl Into<PathBuf>, description: &'static str) -> Self {
        Self {
            path: Some(path.into()),
            description,
        }
    }

    pub fn as_path(&self) -> &Path {
        self.path.as_ref().unwrap().as_path()
    }

    /// Leaves the directory on disk
    pub fn keep(mut self) -> PathBuf {
        self.path.take().unwrap()
    }
}

impl Drop for OwnedDir {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(err) = std::fs::remove_dir_all(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                error = %err,
                path = %path.display(),
                description = self.description,
                "Failed to remove the directory"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
