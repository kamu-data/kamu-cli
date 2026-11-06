// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use odf_storage::get_staging_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A not yet committed file in a repository root.
///
/// Removed on drop unless [`StagingFile::commit()`] moved it into place, so
/// a failed or cancelled write leaves nothing behind.
pub(crate) struct StagingFile {
    path: PathBuf,
    committed: bool,
}

impl StagingFile {
    /// Picks a random staging name in `root`, creating `root` if missing; the
    /// file itself is created by the first write
    // TODO: Cleanup procedure for staging files orphaned by a crash?
    pub(crate) fn new_in(root: &Path) -> std::io::Result<Self> {
        if !root.exists() {
            std::fs::create_dir_all(root)?;
        }

        Ok(Self {
            path: root.join(get_staging_name()),
            committed: false,
        })
    }

    /// Creates the file at `path`, failing with `AlreadyExists` if it exists,
    /// which makes it usable as a lock that only one writer can hold
    pub(crate) fn create_new(path: PathBuf) -> std::io::Result<(Self, std::fs::File)> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        Ok((
            Self {
                path,
                committed: false,
            },
            file,
        ))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Atomically moves the file to `target`, replacing any existing file
    pub(crate) fn commit(mut self, target: &Path) -> std::io::Result<()> {
        std::fs::rename(&self.path, target)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for StagingFile {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Err(err) = std::fs::remove_file(&self.path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = ?self.path, error = ?err, "Failed to remove staging file");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
