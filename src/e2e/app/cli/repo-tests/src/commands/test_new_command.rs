// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_new_root(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["new", "--root", "test-dataset"],
        None,
        Some([indoc::indoc!(
            r#"
            Written new manifest template to: test-dataset.yaml
            Follow directions in the file's comments and use `kamu add test-dataset.yaml` when ready.
            "#
        )]),
    )
    .await;

    // TODO: After solving this issue, add `kamu add` calls and populate with
    //       data
    //
    //       `kamu new`: generate snapshots that will be immediately ready to be
    //       added/worked on
    //       https://github.com/kamu-data/kamu-cli/issues/888
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_new_derivative(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["new", "--derivative", "test-dataset"],
        None,
        Some([indoc::indoc!(
            r#"
            Written new manifest template to: test-dataset.yaml
            Follow directions in the file's comments and use `kamu add test-dataset.yaml` when ready.
            "#
        )]),
    )
    .await;

    // TODO: After solving this issue, add `kamu add` calls and populate with
    //       data
    //
    //       `kamu new`: generate snapshots that will be immediately ready to be
    //       added/worked on
    //       https://github.com/kamu-data/kamu-cli/issues/888
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
