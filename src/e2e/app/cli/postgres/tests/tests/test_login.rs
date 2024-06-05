// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_wrapper::Kamu;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
async fn test_login(pg_pool: PgPool) {
    dbg!(&pg_pool.connect_options());

    let _kamu = Kamu::new_workspace_tmp().await;
}

////////////////////////////////////////////////////////////////////////////////
