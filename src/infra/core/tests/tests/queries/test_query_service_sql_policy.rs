// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use file_utils::OwnedFile;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::ParquetWriterHelper;
use kamu::*;
use kamu_datasets::{MockDatasetActionAuthorizer, ResolvedDataset};
use tempfile::TempDir;

use crate::tests::queries::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_is_read_only_without_policy() {
    let harness = SqlPolicyHarness::new(None).await;

    for (sql, error) in [
        ("CREATE VIEW v AS SELECT 1", "DDL not supported: CreateView"),
        (
            "CREATE TEMP VIEW v AS SELECT 1",
            "DDL not supported: CreateView",
        ),
        (
            "SET datafusion.execution.batch_size = 100",
            "Statement not supported: SetVariable",
        ),
        (
            "COPY (SELECT 1) TO '/tmp/x.csv' STORED AS CSV",
            "DML not supported: COPY",
        ),
    ] {
        let result = harness.sql_statement(sql).await;
        assert!(
            result.as_ref().is_err_and(|e| e.contains(error)),
            "{sql}: {result:?}"
        );
    }

    pretty_assertions::assert_eq!(harness.sql_statement("SELECT * FROM foo").await, Ok(4));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_views() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    harness
        .run(
            &session,
            "CREATE TEMP VIEW v AS SELECT blah FROM foo WHERE offset > 1",
        )
        .await
        .unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM v").await, Ok(2));

    harness
        .run(&session, "CREATE OR REPLACE VIEW v AS SELECT blah FROM foo")
        .await
        .unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM v").await, Ok(4));

    harness.run(&session, "DROP VIEW v").await.unwrap();
    assert!(harness.run(&session, "SELECT * FROM v").await.is_err());

    // Views do not outlive their session
    harness
        .run(&session, "CREATE VIEW w AS SELECT 1")
        .await
        .unwrap();
    let other_session = harness.session().await;
    assert!(
        harness
            .run(&other_session, "SELECT * FROM w")
            .await
            .is_err()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_views_cannot_replace_datasets() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    for (sql, error) in [
        ("CREATE VIEW foo AS SELECT 1", "Table 'foo' already exists"),
        (
            "CREATE OR REPLACE VIEW foo AS SELECT 1",
            "Dataset 'foo' cannot be replaced or dropped",
        ),
        ("DROP VIEW foo", "View 'foo' doesn't exist"),
    ] {
        let result = harness.run(&session, sql).await;
        assert!(
            result.as_ref().is_err_and(|e| e.contains(error)),
            "{sql}: {result:?}"
        );
    }

    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM foo").await, Ok(4));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_rejects_writes() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    for (sql, error) in [
        (
            "COPY (SELECT 1) TO '/tmp/x.csv' STORED AS CSV",
            "DML not supported: COPY",
        ),
        (
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '/tmp/x.csv'",
            "DDL not supported: CreateExternalTable",
        ),
        (
            "CREATE TABLE t AS SELECT 1",
            "DDL not supported: CreateMemoryTable",
        ),
        ("DROP TABLE foo", "DDL not supported: DropTable"),
    ] {
        let result = harness.run(&session, sql).await;
        assert!(
            result.as_ref().is_err_and(|e| e.contains(error)),
            "{sql}: {result:?}"
        );
    }

    // Session statements are allowed
    harness
        .run(&session, "SET datafusion.execution.batch_size = 100")
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_follows_local_policy() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;

    pretty_assertions::assert_eq!(
        harness
            .sql_statement("CREATE TEMP VIEW v AS SELECT 1")
            .await,
        Ok(0)
    );
    assert!(
        harness
            .sql_statement("COPY (SELECT 1) TO '/tmp/x.csv' STORED AS CSV")
            .await
            .is_err_and(|e| e.contains("DML not supported: COPY"))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_schema_refuses_tables_the_policy_does_not_allow() {
    // Plans executed without the policy check still cannot register tables
    let read_only = SqlPolicyHarness::new(None).await;
    let session = read_only.session().await;
    let result = read_only
        .run_unchecked(&session, "CREATE VIEW v AS SELECT 1")
        .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.contains("Views are not allowed in this session")),
        "{result:?}"
    );

    let local = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = local.session().await;
    let result = local
        .run_unchecked(&session, "CREATE TABLE t AS SELECT 1")
        .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.contains("Tables cannot be created in this session")),
        "{result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_session_with_writes_creates_tables() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy {
        allow_session_views: false,
        allow_session_statements: false,
        allow_writes: true,
    }))
    .await;
    let session = harness.session().await;

    harness
        .run(&session, "CREATE TABLE t AS SELECT 1 AS a")
        .await
        .unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM t").await, Ok(1));
    harness.run(&session, "DROP TABLE t").await.unwrap();
    assert!(harness.run(&session, "SELECT * FROM t").await.is_err());

    let input = harness.write_file("in.csv", "a\n1\n2\n");
    harness
        .run(
            &session,
            &format!(
                "CREATE EXTERNAL TABLE e STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' \
                 'true')",
                input.display()
            ),
        )
        .await
        .unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM e").await, Ok(2));

    let output = harness.path("out.csv");
    harness
        .run(
            &session,
            &format!(
                "COPY (SELECT 1 AS a) TO '{}' STORED AS CSV",
                output.display()
            ),
        )
        .await
        .unwrap();
    assert!(output.exists());

    // Datasets stay protected
    for (sql, error) in [
        ("CREATE TABLE foo AS SELECT 1", "Table 'foo' already exists"),
        // `DataFusion` reports any failure to drop a table as a missing table
        ("DROP TABLE foo", "Table 'foo' doesn't exist"),
    ] {
        let result = harness.run(&session, sql).await;
        assert!(
            result.as_ref().is_err_and(|e| e.contains(error)),
            "{sql}: {result:?}"
        );
    }
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM foo").await, Ok(4));

    // Views follow their own rule
    let result = harness.run(&session, "CREATE VIEW v AS SELECT 1").await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.contains("DDL not supported: CreateView")),
        "{result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_view_keeps_definition_of_views_it_reads() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    harness
        .run(&session, "CREATE VIEW base AS SELECT offset, blah FROM foo")
        .await
        .unwrap();
    harness
        .run(
            &session,
            "CREATE VIEW dep AS SELECT blah FROM base WHERE offset < 2",
        )
        .await
        .unwrap();

    // A view stores the plan of its query, so it does not follow later changes
    // to the views it reads
    harness.run(&session, "DROP VIEW base").await.unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM dep").await, Ok(2));

    harness
        .run(
            &session,
            "CREATE VIEW base AS SELECT 99 AS offset, 'z' AS blah",
        )
        .await
        .unwrap();
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM dep").await, Ok(2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_views_live_in_default_schema() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    for sql in [
        "CREATE VIEW kamu.a AS SELECT 1",
        "CREATE VIEW kamu.kamu.b AS SELECT 1",
    ] {
        harness.run(&session, sql).await.unwrap();
    }

    for (sql, error) in [
        (
            "CREATE VIEW other.c AS SELECT 1",
            "failed to resolve schema: other",
        ),
        (
            "CREATE VIEW information_schema.d AS SELECT 1",
            "schema provider does not support registering tables",
        ),
    ] {
        let result = harness.run(&session, sql).await;
        assert!(
            result.as_ref().is_err_and(|e| e.contains(error)),
            "{sql}: {result:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_local_session_views_are_listed() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;
    let session = harness.session().await;

    harness
        .run(&session, "CREATE VIEW v AS SELECT blah FROM foo")
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        harness
            .table(
                &session,
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema \
                 = 'kamu' ORDER BY table_name"
            )
            .await,
        indoc!(
            "
            +------------+------------+
            | table_name | table_type |
            +------------+------------+
            | foo        | BASE TABLE |
            | v          | VIEW       |
            +------------+------------+"
        )
    );
    pretty_assertions::assert_eq!(
        harness
            .table(
                &session,
                "SELECT definition FROM information_schema.views WHERE table_name = 'v'"
            )
            .await,
        indoc!(
            "
            +---------------------------------------+
            | definition                            |
            +---------------------------------------+
            | CREATE VIEW v AS SELECT blah FROM foo |
            +---------------------------------------+"
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_session_reads_datasets_as_of_first_scan() {
    let harness = SqlPolicyHarness::new(Some(SqlPolicy::LOCAL_INTERACTIVE)).await;

    // Data committed before the first scan is read, including by views
    // created earlier
    let session = harness.session().await;
    harness
        .run(&session, "CREATE VIEW v AS SELECT * FROM foo")
        .await
        .unwrap();
    harness.append_to_foo(&[4, 5]).await;
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM v").await, Ok(6));

    // Data committed after it is not, for the rest of the session
    harness.append_to_foo(&[6, 7]).await;
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM v").await, Ok(6));
    pretty_assertions::assert_eq!(harness.run(&session, "SELECT * FROM foo").await, Ok(6));

    // Datasets created after the session started are not visible in it
    harness.create_dataset("bar").await;
    let result = harness.run(&session, "SELECT * FROM bar").await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.contains("table 'kamu.kamu.bar' not found")),
        "{result:?}"
    );

    let new_session = harness.session().await;
    pretty_assertions::assert_eq!(harness.run(&new_session, "SELECT * FROM foo").await, Ok(8));
    pretty_assertions::assert_eq!(harness.run(&new_session, "SELECT * FROM bar").await, Ok(4));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqlPolicyHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    foo: ResolvedDataset,
}

impl SqlPolicyHarness {
    /// Workspace with dataset `foo` of 4 records; `policy` is what the app
    /// would register for the command
    async fn new(policy: Option<SqlPolicy>) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let base_catalog = helpers::create_base_catalog_with_local_workspace(
            tempdir.path(),
            MockDatasetActionAuthorizer::allowing(),
        );

        let mut b = dill::CatalogBuilder::new_chained(&base_catalog);
        b.add::<QueryServiceImpl>();
        if let Some(policy) = policy {
            b.add_value(policy);
        }
        let catalog = b.build();

        let foo = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;

        Self {
            tempdir,
            catalog,
            foo,
        }
    }

    /// Creates a dataset of 4 records, like `foo`
    async fn create_dataset(&self, name: &str) {
        helpers::create_test_dataset(&self.catalog, self.tempdir.path(), name).await;
    }

    /// Commits records with the given offsets to `foo`; they must continue
    /// its last offset
    async fn append_to_foo(&self, offsets: &[u64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("offset", DataType::UInt64, false),
            Field::new("blah", DataType::Utf8, false),
        ]));
        let blah: Vec<String> = offsets.iter().map(|offset| format!("x{offset}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(offsets.to_vec())),
                Arc::new(StringArray::from(blah)),
            ],
        )
        .unwrap();
        let data_path = self.path("append.parquet");
        ParquetWriterHelper::from_record_batch(&data_path, &batch).unwrap();

        let start = offsets[0];
        let end = *offsets.last().unwrap();
        self.foo
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset: Some(start - 1),
                    new_offset_interval: Some(odf::metadata::OffsetInterval { start, end }),
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(data_path)),
                None,
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tempdir.path().join(name)
    }

    fn write_file(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn query_svc(&self) -> Arc<dyn QueryService> {
        self.catalog.get_one::<dyn QueryService>().unwrap()
    }

    /// Runs a statement the way `kamu sql --command` and the API do, and
    /// returns the number of result rows or the error message
    async fn sql_statement(&self, sql: &str) -> Result<usize, String> {
        let response = self
            .query_svc()
            .sql_statement(sql, QueryOptions::default())
            .await
            .map_err(|e| e.to_string())?;

        let batches = response.df.collect().await.map_err(|e| e.to_string())?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
    }

    async fn session(&self) -> SessionContext {
        self.query_svc().create_session().await.unwrap()
    }

    /// Runs a statement in a session the way the SQL shell does, and returns
    /// the number of result rows or the error message
    async fn run(&self, session: &SessionContext, sql: &str) -> Result<usize, String> {
        let df = SqlPolicy::sql(session, sql)
            .await
            .map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
    }

    /// Runs a statement in a session and returns its result as a text table
    async fn table(&self, session: &SessionContext, sql: &str) -> String {
        let batches = SqlPolicy::sql(session, sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    /// Runs a statement without checking it against the session's policy
    async fn run_unchecked(&self, session: &SessionContext, sql: &str) -> Result<(), String> {
        session
            .sql(sql)
            .await
            .map_err(|e| e.to_string())?
            .collect()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
