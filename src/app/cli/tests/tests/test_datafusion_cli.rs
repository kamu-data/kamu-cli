// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////
extern crate assert_fs;
extern crate escargot;

use std::process::Stdio;

#[test]
fn test_datafusion_cli() {
    let _ = env_logger::try_init();

    let child = escargot::CargoBuild::new()
        .bin("kamu-cli")
        .current_release()
        .current_target()
        .run()
        .unwrap()
        .command()
        .arg("-- sql")
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();

    let output = child
        .wait_with_output()
        .expect("Failed to wait for child process");

    let code = output.status.code().unwrap();

    assert_eq!(code, 0);
}
