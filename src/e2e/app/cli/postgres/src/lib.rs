// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////

pub async fn postgres_test_down(pg_pool: PgPool) {
    let connect_options = pg_pool.connect_options();
    let database = connect_options.get_database().unwrap();

    // Stopping a harmless idle transaction from kamu.
    // Also removes a warning in the logs from sqlx-test
    //
    // Background: PostgresSQL does not allow us to delete a database
    //             without errors if there are connections to it,
    //             even if they are idle
    sqlx::query!(
        r#"
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = $1
          AND state = 'idle'
          AND pid <> pg_backend_pid()   -- exclude the current connection
          AND query = 'COMMIT'          -- last executed query
          AND wait_event = 'ClientRead' -- awaiting new data
        "#,
        database
    )
    .fetch_one(&pg_pool)
    .await
    .unwrap();

    let stale_transaction_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as count
        FROM pg_stat_activity
        WHERE datname = $1
          AND state <> 'idle'
          AND pid <> pg_backend_pid()   -- exclude the current connection
          AND query <> 'COMMIT'          -- last executed query
          AND wait_event <> 'ClientRead' -- awaiting new data
        "#,
        database
    )
    .fetch_one(&pg_pool)
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        0, stale_transaction_count,
        "Stale transactions are present!"
    );
}
