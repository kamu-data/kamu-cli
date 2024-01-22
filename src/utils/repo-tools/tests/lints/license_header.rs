// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

fn get_repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../")
        .canonicalize()
        .unwrap()
}

fn get_all_crates() -> Vec<PathBuf> {
    let repo_root = get_repo_root();

    let root_cargo_content = std::fs::read_to_string(repo_root.join("Cargo.toml"))
        .expect("Could not read root Cargo.toml file");

    let root_cargo: toml::Value = root_cargo_content
        .parse()
        .expect("Failed to parse root Cargo.toml");

    root_cargo["workspace"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .map(|s| repo_root.join(s))
        .collect()
}

#[test]
fn check_all_files_have_license_header() {
    let repo_root = get_repo_root();

    let header = std::fs::read_to_string(repo_root.join("docs/license_header.txt")).unwrap();
    let header = header.trim();

    let mut bad_files = Vec::new();

    for crate_path in get_all_crates() {
        let pattern = crate_path.join("**").join("*.rs");

        for entry in glob::glob(pattern.to_str().unwrap()).unwrap() {
            let file = entry.unwrap();
            let file_rel = file.strip_prefix(&repo_root).unwrap().to_owned();

            eprintln!("Checking file: {}", file_rel.display());

            let content = std::fs::read_to_string(&file).unwrap();
            if !content.starts_with(header) {
                bad_files.push(file_rel);
            }
        }
    }

    if !bad_files.is_empty() {
        eprintln!("License header is missing from following files:");
        for f in bad_files {
            eprintln!("- {}", f.display());
        }
        panic!("License file is missing in some files");
    }
}
