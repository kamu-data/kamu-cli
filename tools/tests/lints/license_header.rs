// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

#[test]
fn check_all_files_have_license_header() {
    let header = indoc::indoc!(
        r"
        // Copyright Kamu Data, Inc. and contributors. All rights reserved.
        //
        // Use of this software is governed by the Business Source License
        // included in the LICENSE file.
        //
        // As of the Change Date specified in that file, in accordance with
        // the Business Source License, use of this software will be governed
        // by the Apache License, Version 2.0.

        "
    );
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .unwrap();

    let pattern = repo_root.join("**").join("*.rs");

    let mut bad_files = Vec::new();

    for entry in glob::glob(pattern.to_str().unwrap()).unwrap() {
        let file = entry.unwrap();
        let file_rel = file.strip_prefix(&repo_root).unwrap().to_owned();

        if !file_rel.starts_with("target") {
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
        assert!(false, "License file is missing in some files");
    }
}
