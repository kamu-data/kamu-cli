// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use sqlx::SqlitePool;
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("../../../migrations/sqlite");
pub const DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME: &str = "workspace.sqlite.db";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlitePlugin {}

#[component(pub)]
impl SqlitePlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(catalog_builder: &mut CatalogBuilder) {
        catalog_builder.add::<Self>();
        catalog_builder.add::<SqliteTransactionManager>();
    }

    pub async fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        db_connection_settings: &DatabaseConnectionSettings,
    ) -> Result<Catalog, DatabaseInitError> {
        let sqlite_pool = Self::open_sqlite_pool(db_connection_settings);

        Self::run_migrations(&sqlite_pool).await?;

        // Do not accumulate changes related to migrations and save them to the database
        // immediately
        Self::force_wal_checkpoints_save(&sqlite_pool)
            .await
            .int_err()?;

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(sqlite_pool)
            .build())
    }

    async fn run_migrations(sqlite_pool: &SqlitePool) -> Result<(), DatabaseInitError> {
        let Err(err) = SQLITE_MIGRATOR.run(sqlite_pool).await else {
            return Ok(());
        };

        // sqlx validates applied migrations before applying any, so a schema written by
        // a newer build is reported here and left untouched
        let latest_known_version = Self::latest_known_migration_version();
        if let MigrateError::VersionMissing(missing_version) = err
            && missing_version > latest_known_version
        {
            // A runtime query: `_sqlx_migrations` belongs to sqlx, and this crate has no
            // compile-time checked schema
            let latest_applied_version: i64 =
                sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
                    .fetch_one(sqlite_pool)
                    .await
                    .int_err()?;

            return Err(DatabaseSchemaTooNewError {
                latest_applied_version,
                latest_known_version,
            }
            .into());
        }

        Err(err.int_err().into())
    }

    fn latest_known_migration_version() -> i64 {
        SQLITE_MIGRATOR
            .iter()
            .map(|migration| migration.version)
            .max()
            .unwrap_or_default()
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_sqlite_pool(db_connection_settings: &DatabaseConnectionSettings) -> SqlitePool {
        let sqlite_options = SqliteConnectOptions::new()
            .filename(&db_connection_settings.host)
            .create_if_missing(true)
            // Protection against locking, in case of parallel execution of kamu commands.
            //
            // Docs:
            // - Write-Ahead Logging https://www.sqlite.org/wal.html
            // - Pragma statements supported by SQLite https://www.sqlite.org/pragma.html#pragma_journal_mode
            .pragma("journal_mode", "WAL");

        if db_connection_settings.max_lifetime_secs.is_some() {
            tracing::warn!("max_lifetime_secs is not supported for SQLite");
        }
        if db_connection_settings.max_connections.is_some() {
            tracing::warn!("max_connections is not supported for SQLite");
        }
        if db_connection_settings.acquire_timeout_secs.is_some() {
            tracing::warn!("acquire_timeout_secs is not supported for SQLite");
        }

        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_lazy_with(sqlite_options)
    }

    pub async fn force_wal_checkpoints_save(sqlite_pool: &SqlitePool) -> Result<(), DatabaseError> {
        // Based on:
        // java - Android sqlite flush WAL file contents into main database file
        // https://stackoverflow.com/questions/30276789/android-sqlite-flush-wal-file-contents-into-main-database-file/69028219#69028219

        use sqlx::{Acquire, Executor};

        let mut pool = sqlite_pool.acquire().await?;
        let connection_mut = pool.acquire().await?;
        connection_mut
            .execute("PRAGMA wal_checkpoint(full)")
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
