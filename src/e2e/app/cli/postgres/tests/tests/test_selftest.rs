// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::e2e_test;
use kamu_cli_wrapper::Kamu;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////

#[test_group::group(e2e, database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
async fn test_selftest(pg_pool: PgPool) {
    let db = pg_pool.connect_options();
    let kamu = Kamu::new_workspace_tmp_with_kamu_config(
        format!(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                    database:
                        provider: postgres
                        host: {host}
                        user: {user}
                        password: {password}
                        databaseName: {database}
                "#
            ),
            host = db.get_host(),
            user = db.get_username(),
            password = db.get_username(), // It's intended: password is same as user for tests
            database = db.get_database().unwrap(),
        )
        .as_str(),
    )
    .await;

    e2e_test(kamu, |kamu_api_server_client| async {
        kamu_cli_e2e_repo_tests::test_selftest(kamu_api_server_client).await;
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////
