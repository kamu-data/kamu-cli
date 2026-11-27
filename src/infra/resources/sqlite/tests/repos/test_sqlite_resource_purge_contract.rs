// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_repo_tests::{ResourceKeyedColumn, purge_contract_violations};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Per backend: the contract is checked against the migrated schema, which the
// in-memory repositories do not have

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_every_resource_keyed_table_is_purged_with_the_resource(pool: SqlitePool) {
    let harness = PurgeContractHarness::new(pool);

    let violations = purge_contract_violations(&harness.resource_keyed_columns().await);

    assert_eq!(violations, Vec::<String>::new());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_purge_contract_rejects_an_unregistered_resource_keyed_table(pool: SqlitePool) {
    let harness = PurgeContractHarness::new(pool);
    harness
        .create_table(
            "CREATE TABLE test_cascading_projection (resource_id CHAR(36) NOT NULL REFERENCES \
             resources(resource_id) ON DELETE CASCADE)",
        )
        .await;
    harness
        .create_table(
            "CREATE TABLE test_unregistered_projection (owner_resource_id CHAR(36) NOT NULL)",
        )
        .await;

    let violations = purge_contract_violations(&harness.resource_keyed_columns().await);

    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].starts_with("test_unregistered_projection.owner_resource_id "),
        "{violations:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PurgeContractHarness {
    pool: SqlitePool,
}

impl PurgeContractHarness {
    fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn create_table(&self, sql: &'static str) {
        sqlx::raw_sql(sql).execute(&self.pool).await.unwrap();
    }

    async fn resource_keyed_columns(&self) -> Vec<ResourceKeyedColumn> {
        let rows: Vec<(String, String, bool)> = sqlx::query_as(
            r#"
            SELECT
                m.name,
                p.name,
                EXISTS (
                    SELECT 1
                    FROM pragma_foreign_key_list(m.name) AS f
                    WHERE f."from" = p.name
                      AND f."table" = 'resources'
                      AND f.on_delete = 'CASCADE'
                ) AS cascades
            FROM sqlite_master AS m
            JOIN pragma_table_info(m.name) AS p
            WHERE m.type = 'table'
              AND m.name <> 'resources'
              AND (p.name = 'resource_id' OR p.name GLOB '*_resource_id')
            ORDER BY 1, 2
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        rows.into_iter()
            .map(|(table, column, cascades)| ResourceKeyedColumn::new(table, column, cascades))
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
