// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeDelta, Utc};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

use crate::resources::fixtures;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: a deleted resource is purged after a command once its retention
// ended
//
// Each command runs one purge batch after its own work, so the command that
// first finds the resource past its deadline still sees it, and the next one
// does not.
//
// IMPORTANT: wire it with
// `Options::with_kamu_config(fixtures::RETENTION_ONE_DAY_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_purged_after_retention(mut kamu: KamuCliPuppet) {
    let deleted_on: DateTime<Utc> = "2050-01-02T03:04:05Z".parse().unwrap();
    kamu.set_system_time(Some(deleted_on));

    kamu.execute_with_input(
        ["apply", "--stdin"],
        fixtures::variable_set_manifest_yaml("purged-vars", "one"),
    )
    .await
    .success();
    kamu.execute(["delete", "vs", "purged-vars", "--force"])
        .await
        .success();

    let get_deleted = ["get", "vs", "purged-vars", "--include-deleted"];
    let not_found = "Resource 'purged-vars' of type 'VariableSet' was not found";

    kamu.set_system_time(Some(deleted_on + TimeDelta::hours(23)));
    kamu.execute(get_deleted).await.success();
    kamu.execute(get_deleted).await.success();

    // Past the deadline: purged after the first command that finds it so
    kamu.set_system_time(Some(deleted_on + TimeDelta::hours(25)));
    kamu.execute(get_deleted).await.success();
    kamu.assert_failure_command_execution(get_deleted, None, Some([not_found]))
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
