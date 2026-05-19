// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

pub struct ExampleWorkspace {
    pub root: PathBuf,
}

impl ExampleWorkspace {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn new_by_name_with_clean_state(name: &str) -> Self {
        let root = Self::example_dir(name);
        assert!(root.is_dir(), "Cannot find example at {}", root.display());
        let s = Self::new(root);
        s.cleanup();
        s.init();
        s
    }

    pub fn example_dir(name: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../../../../../examples");
        path.push(name);
        path
    }

    pub fn init(&self) {
        std::process::Command::new("kamu")
            .args(["init"])
            .current_dir(&self.root)
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();
    }

    pub fn cleanup(&self) {
        let ws = self.root.join(".kamu");
        if ws.is_dir() {
            std::fs::remove_dir_all(ws).unwrap();
        }
    }
}

impl Drop for ExampleWorkspace {
    fn drop(&mut self) {
        self.cleanup();
    }
}
