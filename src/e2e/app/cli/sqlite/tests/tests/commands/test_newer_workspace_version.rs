// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = scenarios::test_database_from_newer_version_is_reported
);

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = scenarios::test_workspace_layout_from_newer_version_is_reported
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mod scenarios {
    use database_common::DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME;
    use kamu_cli_e2e_common::DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR;
    use kamu_cli_puppet::KamuCliPuppet;
    use kamu_cli_puppet::extensions::KamuCliPuppetExt;
    use kamu_core::KAMU_WORKSPACE_DIR_NAME;

    const FUTURE_MIGRATION_VERSION: i64 = 99_991_231_235_959;

    pub async fn test_database_from_newer_version_is_reported(kamu: KamuCliPuppet) {
        kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
            .await
            .success();

        record_future_migration(&kamu).await;

        kamu.assert_failure_command_execution(
            ["list"],
            None,
            Some([
                r"Workspace database was created by a newer version of kamu \(schema version 99991231235959, this version supports up to \d+\) - please upgrade kamu to the latest version",
            ]),
        )
        .await;
    }

    pub async fn test_workspace_layout_from_newer_version_is_reported(kamu: KamuCliPuppet) {
        std::fs::write(
            kamu.workspace_path()
                .join(KAMU_WORKSPACE_DIR_NAME)
                .join("version"),
            "999",
        )
        .unwrap();

        kamu.assert_failure_command_execution(
            ["list"],
            None,
            Some(["Workspace version 999 is newer than supported version"]),
        )
        .await;
    }

    /// Makes the workspace database look as if a newer version of kamu
    /// migrated it
    async fn record_future_migration(kamu: &KamuCliPuppet) {
        let database_path = kamu
            .workspace_path()
            .join(KAMU_WORKSPACE_DIR_NAME)
            .join(DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME);
        let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", database_path.display()))
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES ($1, 'future migration', TRUE, x'00', 0)",
        )
        .bind(FUTURE_MIGRATION_VERSION)
        .execute(&pool)
        .await
        .unwrap();

        pool.close().await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
