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

pub async fn test_gc(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["system", "gc"],
        None,
        Some(["Cleaning cache...", "Workspace is already clean"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
