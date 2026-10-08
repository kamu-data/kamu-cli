// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::PathBuf;

use database_common::{
    DatabaseConnectionSettings,
    DatabaseInitError,
    DatabaseSchemaTooNewError,
    SQLITE_MIGRATOR,
    SqlitePlugin,
};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_connect_applies_migrations_to_new_database() {
    let harness = SqlitePluginHarness::new();

    let res = harness.connect().await;

    assert_matches!(res, Ok(_));
    assert_eq!(
        harness.latest_applied_version().await,
        SqlitePluginHarness::latest_known_version()
    );
}

#[tokio::test]
async fn test_connect_reconnects_to_up_to_date_database() {
    let harness = SqlitePluginHarness::new();
    harness.connect().await.unwrap();

    let res = harness.connect().await;

    assert_matches!(res, Ok(_));
}

#[tokio::test]
async fn test_connect_reports_schema_written_by_newer_version() {
    let harness = SqlitePluginHarness::new();
    harness.connect().await.unwrap();
    let future_version = SqlitePluginHarness::latest_known_version() + 1;
    harness.record_applied_migration(future_version).await;

    let res = harness.connect().await;

    assert_matches!(
        res,
        Err(DatabaseInitError::SchemaTooNew(DatabaseSchemaTooNewError {
            latest_applied_version,
            latest_known_version,
        })) if latest_applied_version == future_version
            && latest_known_version == SqlitePluginHarness::latest_known_version()
    );
    assert_eq!(harness.latest_applied_version().await, future_version);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqlitePluginHarness {
    database_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl SqlitePluginHarness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        Self {
            database_path: tmp.path().join("workspace.sqlite.db"),
            _tmp: tmp,
        }
    }

    fn latest_known_version() -> i64 {
        SQLITE_MIGRATOR.iter().map(|m| m.version).max().unwrap()
    }

    async fn connect(&self) -> Result<dill::Catalog, DatabaseInitError> {
        SqlitePlugin::catalog_with_connected_pool(
            &dill::CatalogBuilder::new().build(),
            &DatabaseConnectionSettings::sqlite_from(&self.database_path),
        )
        .await
    }

    async fn pool(&self) -> SqlitePool {
        SqlitePool::connect(&format!("sqlite://{}", self.database_path.display()))
            .await
            .unwrap()
    }

    async fn record_applied_migration(&self, version: i64) {
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES ($1, 'future migration', TRUE, x'00', 0)",
        )
        .bind(version)
        .execute(&self.pool().await)
        .await
        .unwrap();
    }

    async fn latest_applied_version(&self) -> i64 {
        sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&self.pool().await)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
