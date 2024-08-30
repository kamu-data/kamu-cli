// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Preparing the build information
    vergen::EmitBuilder::builder()
        .all_build()
        .all_git()
        .all_rustc()
        .all_cargo()
        .fail_on_error()
        .emit()?;

    // sqlx will cause kamu-cli to be rebuilt if already embedded migrations have
    // changed.
    //
    // But if the migration was previously missing, the rebuild would not be
    // triggered.
    //
    // To solve this, we tell the compiler to perform a rebuild
    // in case of the migrations folder content has changed.
    //
    // NB. Working path: ./src/app/cli
    println!("cargo:rerun-if-changed=../../../migrations/sqlite");

    Ok(())
}
