// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli::{DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME, KAMU_WORKSPACE_DIR_NAME};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_multi_tenant_creates_sqlite_database(mut kamu: KamuCliPuppet) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(["init", "--multi-tenant"]).await.success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME);

    assert!(expected_database_path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_multi_tenant_with_exists_ok_flag_creates_sqlite_database(
    mut kamu: KamuCliPuppet,
) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(["init", "--multi-tenant", "--exists-ok"])
        .await
        .success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME);

    assert!(expected_database_path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_exist_ok_st(mut kamu: KamuCliPuppet) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(["init"]).await.success();
    kamu.execute(["init", "--exists-ok"]).await.success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_exist_ok_mt(mut kamu: KamuCliPuppet) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(["init", "--multi-tenant"]).await.success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME);

    let modified_old = expected_database_path
        .metadata()
        .unwrap()
        .modified()
        .unwrap();

    kamu.execute(["init", "--multi-tenant", "--exists-ok"])
        .await
        .success();

    let modified_new = expected_database_path
        .metadata()
        .unwrap()
        .modified()
        .unwrap();

    // Verify that the database has not been overwritten
    pretty_assertions::assert_eq!(modified_old, modified_new);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_in_an_existing_workspace(mut kamu: KamuCliPuppet) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.assert_success_command_execution(["init"], None, Some(["Initialized an empty workspace"]))
        .await;

    kamu.assert_failure_command_execution(
        ["init"],
        None,
        Some(["Error: Directory is already a kamu workspace"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
