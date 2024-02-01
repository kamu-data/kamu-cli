// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////
extern crate escargot;
extern crate assert_fs;
use std::process::{Stdio};

#[test_log::test(tokio::test)]
async fn test_datafusion_cli() {
    let run = escargot::CargoBuild::new()
        .bin("kamu-cli")
        .current_release()
        .current_target()
        .run()
        .unwrap()
        .command()
        .arg("sql")
        .stdin(Stdio::piped())
        .status()
        .unwrap();

    assert_eq!(run.success(), true);
}
