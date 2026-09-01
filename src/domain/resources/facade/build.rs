// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let schema_path = manifest_dir
        .join("../../../../resources/schema.gql")
        .canonicalize()
        .unwrap();

    println!("cargo:rerun-if-changed={}", schema_path.display());

    let schema_contents = std::fs::read_to_string(&schema_path).unwrap();
    let schema_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        schema_contents.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    };

    let hash_marker_path = out_dir.join("schema.gql.hash");
    let previous_hash = std::fs::read_to_string(&hash_marker_path).ok();

    // `cynic_codegen::register_schema` unconditionally rewrites its generated
    // files in `OUT_DIR` on every invocation, which bumps their mtime and
    // forces a recompile of dependents even when the schema content is
    // unchanged (e.g. after a `touch` or checkout that only updates mtime).
    // Skip regeneration when the content hash matches the previous run.
    if previous_hash.as_deref() != Some(schema_hash.as_str()) {
        cynic_codegen::register_schema("kamu")
            .from_sdl_file(schema_path)
            .unwrap()
            .as_default()
            .unwrap();

        std::fs::write(&hash_marker_path, &schema_hash).unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
