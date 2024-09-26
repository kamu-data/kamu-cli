// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli::{DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME, KAMU_WORKSPACE_DIR_NAME};
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

pub async fn test_init_exist_ok_st(mut kamu: KamuCliPuppet) {
    kamu.set_workspace_path_in_tmp_dir();

    kamu.execute(["init"]).await.success();
    kamu.execute(["init", "--exists-ok"]).await.success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
