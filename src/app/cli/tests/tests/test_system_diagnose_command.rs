// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli::*;

#[test_log::test(tokio::test)]
async fn test_system_diagnose() {
    assert_matches!(
        SystemDiagnose::check().await,
        SystemDiagnose {
            run_check: RunCheck {
                container_runtime_type: _,
                is_installed: _,
                is_rootless: _,
                workspace_dir: Some(_),
                is_workspace_consitence: _,
            }
        }
    )
}
