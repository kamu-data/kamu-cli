// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_repo_tests::{ResourceKeyedColumn, purge_contract_violations};
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_every_resource_keyed_table_is_purged_with_the_resource(pool: PgPool) {
    let harness = PurgeContractHarness::new(pool);

    let violations = purge_contract_violations(&harness.resource_keyed_columns().await);

    assert_eq!(violations, Vec::<String>::new());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_purge_contract_rejects_an_unregistered_resource_keyed_table(pool: PgPool) {
    let harness = PurgeContractHarness::new(pool);
    harness
        .create_table(
            "CREATE TABLE test_cascading_projection (resource_id uuid NOT NULL REFERENCES \
             resources(resource_id) ON DELETE CASCADE)",
        )
        .await;
    harness
        .create_table("CREATE TABLE test_unregistered_projection (owner_resource_id uuid NOT NULL)")
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
    pool: PgPool,
}

impl PurgeContractHarness {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn create_table(&self, sql: &'static str) {
        sqlx::raw_sql(sql).execute(&self.pool).await.unwrap();
    }

    async fn resource_keyed_columns(&self) -> Vec<ResourceKeyedColumn> {
        let rows: Vec<(String, String, bool)> = sqlx::query_as(
            r#"
            SELECT
                c.table_name::text,
                c.column_name::text,
                EXISTS (
                    SELECT 1
                    FROM pg_constraint AS con
                    JOIN pg_attribute AS a
                        ON a.attrelid = con.conrelid AND a.attnum = ANY (con.conkey)
                    WHERE con.contype = 'f'
                      AND con.conrelid = format('%I.%I', c.table_schema, c.table_name)::regclass
                      AND con.confrelid = 'resources'::regclass
                      AND con.confdeltype = 'c'
                      AND a.attname = c.column_name
                ) AS cascades
            FROM information_schema.columns AS c
            JOIN information_schema.tables AS t
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
                AND t.table_type = 'BASE TABLE'
            WHERE c.table_schema = current_schema()
              AND c.table_name <> 'resources'
              AND (c.column_name = 'resource_id' OR c.column_name LIKE '%\_resource\_id')
            ORDER BY 1, 2
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .unwrap();

        rows.into_iter()
            .map(|(table, column, cascades)| ResourceKeyedColumn {
                table,
                column,
                cascades,
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
