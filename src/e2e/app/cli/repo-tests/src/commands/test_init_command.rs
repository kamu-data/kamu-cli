// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::TenancyConfig;
use kamu_cli::{DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME, KAMU_WORKSPACE_DIR_NAME};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_init_creates_sqlite_database
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_creates_sqlite_database_st(kamu: KamuCliPuppet) {
    test_init_creates_sqlite_database(kamu, TenancyConfig::SingleTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_creates_sqlite_database_mt(kamu: KamuCliPuppet) {
    test_init_creates_sqlite_database(kamu, TenancyConfig::MultiTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_init_with_exists_ok_flag_creates_sqlite_database
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_with_exists_ok_flag_creates_sqlite_database_st(kamu: KamuCliPuppet) {
    test_init_with_exists_ok_flag_creates_sqlite_database(kamu, TenancyConfig::SingleTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_with_exists_ok_flag_creates_sqlite_database_mt(kamu: KamuCliPuppet) {
    test_init_with_exists_ok_flag_creates_sqlite_database(kamu, TenancyConfig::MultiTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_init_exist_ok
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_exist_ok_st(kamu: KamuCliPuppet) {
    test_init_exist_ok(kamu, TenancyConfig::SingleTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_exist_ok_mt(kamu: KamuCliPuppet) {
    test_init_exist_ok(kamu, TenancyConfig::MultiTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_init_in_an_existing_workspace
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_in_an_existing_workspace_st(kamu: KamuCliPuppet) {
    test_init_in_an_existing_workspace(kamu, TenancyConfig::SingleTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_init_in_an_existing_workspace_mt(kamu: KamuCliPuppet) {
    test_init_in_an_existing_workspace(kamu, TenancyConfig::MultiTenant).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_init_creates_sqlite_database(mut kamu: KamuCliPuppet, tenant_config: TenancyConfig) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(match tenant_config {
        TenancyConfig::SingleTenant => vec!["init"],
        TenancyConfig::MultiTenant => vec!["init", "--multi-tenant"],
    })
    .await
    .success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME);

    assert!(expected_database_path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_init_with_exists_ok_flag_creates_sqlite_database(
    mut kamu: KamuCliPuppet,
    tenant_config: TenancyConfig,
) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(match tenant_config {
        TenancyConfig::SingleTenant => vec!["init", "--exists-ok"],
        TenancyConfig::MultiTenant => vec!["init", "--multi-tenant", "--exists-ok"],
    })
    .await
    .success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME);

    assert!(expected_database_path.exists());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_init_exist_ok(mut kamu: KamuCliPuppet, tenant_config: TenancyConfig) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(match tenant_config {
        TenancyConfig::SingleTenant => vec!["init"],
        TenancyConfig::MultiTenant => vec!["init", "--multi-tenant"],
    })
    .await
    .success();

    let expected_database_path = kamu
        .workspace_path()
        .join(KAMU_WORKSPACE_DIR_NAME)
        .join(DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME);

    let modified_old = expected_database_path
        .metadata()
        .unwrap()
        .modified()
        .unwrap();

    kamu.execute(match tenant_config {
        TenancyConfig::SingleTenant => vec!["init", "--exists-ok"],
        TenancyConfig::MultiTenant => vec!["init", "--multi-tenant", "--exists-ok"],
    })
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

async fn test_init_in_an_existing_workspace(mut kamu: KamuCliPuppet, tenant_config: TenancyConfig) {
    kamu.set_workspace_path_in_tmp_dir();

    let command = match tenant_config {
        TenancyConfig::SingleTenant => vec!["init"],
        TenancyConfig::MultiTenant => vec!["init", "--multi-tenant"],
    };

    kamu.assert_success_command_execution(
        &command,
        None,
        Some(match tenant_config {
            TenancyConfig::SingleTenant => vec!["Initialized an empty workspace"],
            TenancyConfig::MultiTenant => vec!["Initialized an empty multi-tenant workspace"],
        }),
    )
    .await;

    kamu.assert_failure_command_execution(
        &command,
        None,
        Some(["Error: Directory is already a kamu workspace"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
