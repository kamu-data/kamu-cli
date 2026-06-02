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

    let schema_path = manifest_dir
        .join("../../../../resources/schema.gql")
        .canonicalize()
        .unwrap();

    println!("cargo:rerun-if-changed={}", schema_path.display());

    cynic_codegen::register_schema("kamu")
        .from_sdl_file(schema_path)
        .unwrap()
        .as_default()
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
