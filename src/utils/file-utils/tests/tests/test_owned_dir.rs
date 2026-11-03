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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
