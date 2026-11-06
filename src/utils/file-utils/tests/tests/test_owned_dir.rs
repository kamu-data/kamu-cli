// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use file_utils::OwnedDir;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_owned_dir_removed_on_drop() {
    let harness = OwnedDirHarness::new();
    let path = harness.make_dir_with_file("run");

    drop(OwnedDir::new(&path, "test directory"));

    assert!(!path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_owned_dir_kept() {
    let harness = OwnedDirHarness::new();
    let path = harness.make_dir_with_file("run");

    let kept = OwnedDir::new(&path, "test directory").keep();

    assert_eq!(kept, path);
    assert!(path.join("file").exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_owned_dir_already_gone() {
    let harness = OwnedDirHarness::new();
    let path = harness.missing_path("run");

    drop(OwnedDir::new(&path, "test directory"));

    assert!(!path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(unix)]
#[test]
fn test_owned_dir_removal_failure_does_not_panic() {
    let harness = OwnedDirHarness::new();
    let Some(path) = harness.make_undeletable_dir("run") else {
        // Permissions do not bind this user, e.g. root
        return;
    };

    drop(OwnedDir::new(&path, "test directory"));

    assert!(path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct OwnedDirHarness {
    temp_dir: tempfile::TempDir,
}

impl OwnedDirHarness {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
        }
    }

    /// A directory holding one file
    fn make_dir_with_file(&self, name: &str) -> PathBuf {
        let path = self.temp_dir.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("file"), b"contents").unwrap();
        path
    }

    fn missing_path(&self, name: &str) -> PathBuf {
        self.temp_dir.path().join(name)
    }

    /// A directory inside a read-only parent, so removing it fails with a
    /// permission error. `None` when the read-only parent still accepts
    /// writes, as it does for root.
    #[cfg(unix)]
    fn make_undeletable_dir(&self, name: &str) -> Option<PathBuf> {
        use std::os::unix::fs::PermissionsExt;

        let parent = self.locked_parent_path();
        std::fs::create_dir(&parent).unwrap();
        let path = parent.join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();

        let probe = parent.join("probe");
        if std::fs::write(&probe, b"").is_ok() {
            std::fs::remove_file(probe).unwrap();
            return None;
        }
        Some(path)
    }

    fn locked_parent_path(&self) -> PathBuf {
        self.temp_dir.path().join("locked")
    }
}

impl Drop for OwnedDirHarness {
    /// Unlocks the read-only parent, so the temporary directory can be
    /// removed
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let parent = self.locked_parent_path();
            if parent.exists() {
                std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
