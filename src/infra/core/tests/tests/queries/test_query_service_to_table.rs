// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::prelude::*;
use indoc::indoc;
use kamu::domain::*;
use kamu::*;
use kamu_datasets::{DatasetRegistry, MockDatasetPermissionChecker, ResolvedDataset};
use kamu_ingest_datafusion::{DataWriterDataFusion, ReaderCsv};
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use odf::utils::testing::assert_data_eq;
use time_source::SystemTimeSource;

use crate::tests::queries::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// `to_table()` returns the state projected from a dataset's changelog: the latest record per
// primary key by offset, without retracted keys
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_several_changes_per_key_in_one_block() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                1,A,1000
                0,A,2000
                0,B,10
                2,B,10
                3,B,20
                2,B,20
                3,B,30
                0,C,1
                1,C,1
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 2      | 0  | A    | 2000       |
            | 7      | 3  | B    | 30         |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_across_blocks() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,2000
                0,C,3000
                "#
            ),
            schema,
        )
        .await;

    // Corrections and retractions refer to records of the previous block
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                2,A,1000
                3,A,1100
                1,B,2000
                "#
            ),
            schema,
        )
        .await;

    // A retracted key appears again, a live key is retracted
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,B,2100
                1,C,3000
                "#
            ),
            schema,
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 4      | 3  | A    | 1100       |
            | 6      | 0  | B    | 2100       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_composite_primary_key() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            ToTableHarness::changelog_stream(&["city", "year"]),
            None,
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,year,population
                0,A,2020,1000
                0,A,2021,1100
                1,A,2020,1000
                0,B,2020,2000
                2,B,2020,2000
                3,B,2020,2500
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, year INT NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, year, population FROM to_table(foo) ORDER BY city, year")
            .await,
        indoc!(
            r#"
            +--------+----+------+------+------------+
            | offset | op | city | year | population |
            +--------+----+------+------+------------+
            | 1      | 0  | A    | 2021 | 1100       |
            | 5      | 3  | B    | 2020 | 2500       |
            +--------+----+------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_custom_vocabulary() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            ToTableHarness::changelog_stream(&["city"]),
            Some(odf::metadata::SetVocab {
                offset_column: Some("idx".to_string()),
                operation_type_column: Some("change".to_string()),
                ..Default::default()
            }),
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                change,city,population
                0,A,1000
                0,B,2000
                1,A,1000
                2,B,2000
                3,B,2500
                "#
            ),
            "change INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT idx, change, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +-----+--------+------+------------+
            | idx | change | city | population |
            +-----+--------+------+------------+
            | 4   | 3      | B    | 2500       |
            +-----+--------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_filter_on_non_key_column_applies_to_current_state() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,10
                0,C,5
                "#
            ),
            schema,
        )
        .await;

    // Every newer record fails the filter below, while the records they replace pass it.
    // Filtering before projecting would bring the stale ones back as the current state.
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                2,A,1000
                3,A,2000
                1,C,5
                "#
            ),
            schema,
        )
        .await;

    assert_data_eq(
        harness
            .sql(
                "SELECT offset, op, city, population FROM to_table(foo) WHERE population < 1500 \
                 ORDER BY city",
            )
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 1      | 0  | B    | 10         |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;

    assert_data_eq(
        harness
            .sql(
                "SELECT COUNT(*) AS num, SUM(population) AS total FROM to_table(foo) WHERE \
                 population < 1500",
            )
            .await,
        indoc!(
            r#"
            +-----+-------+
            | num | total |
            +-----+-------+
            | 1   | 10    |
            +-----+-------+
            "#
        ),
    )
    .await;

    // Filters on the primary key select whole keys, so they see the current state too
    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, population FROM to_table(foo) WHERE city IN ('A', 'C')")
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 4      | 3  | A    | 2000       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_snapshot_from_push_source() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset_with_source(
            "foo",
            MetadataFactory::add_push_source()
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build()
                .into(),
            None,
        )
        .await;
    let schema = "city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                "#
            ),
            schema,
        )
        .await;

    // `A` changes, `B` disappears, `D` appears
    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1100
                C,3000
                D,4000
                "#
            ),
            schema,
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +----+------+------------+
            | op | city | population |
            +----+------+------------+
            | 3  | A    | 1100       |
            | 0  | C    | 3000       |
            | 0  | D    | 4000       |
            +----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_by_alias_and_id_return_same_state() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                2,A,1000
                3,A,1100
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    let expected = indoc!(
        r#"
        +--------+------+------------+
        | offset | city | population |
        +--------+------+------------+
        | 2      | A    | 1100       |
        +--------+------+------------+
        "#
    );

    assert_data_eq(
        harness
            .sql("SELECT offset, city, population FROM to_table(foo)")
            .await,
        expected,
    )
    .await;
    assert_data_eq(
        harness
            .sql(&format!(
                "SELECT offset, city, population FROM to_table(`{}`)",
                foo.get_id()
            ))
            .await,
        expected,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_follows_input_dataset_block_hash() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    let first_head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            schema,
        )
        .await;
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                1,A,1000
                0,A,2000
                "#
            ),
            schema,
        )
        .await;

    // The changelog and its projection must see the same state of the dataset
    let res = harness
        .sql_as_of(
            "SELECT city, population FROM to_table(foo)",
            &foo,
            first_head,
        )
        .await;

    assert_data_eq(
        res,
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            +------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_dataset_is_part_of_query_state() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    let head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // The state must pin every dataset the result depends on to be reproducible
    let state = harness.sql_state("SELECT * FROM to_table(foo)").await;

    assert_eq!(
        state.input_datasets,
        BTreeMap::from([(
            foo.get_id().clone(),
            QueryStateDataset {
                alias: "foo".to_string(),
                block_hash: head,
            }
        )])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_in_expression_subqueries() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    let head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,2000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    for (statement, expected) in [
        (
            "SELECT (SELECT count(*) FROM to_table(foo)) AS n",
            indoc!(
                r#"
                +---+
                | n |
                +---+
                | 2 |
                +---+
                "#
            ),
        ),
        (
            "SELECT 1 AS n WHERE EXISTS (SELECT 1 FROM to_table(foo) WHERE city = 'B')",
            indoc!(
                r#"
                +---+
                | n |
                +---+
                | 1 |
                +---+
                "#
            ),
        ),
        (
            "SELECT x FROM (VALUES ('A'), ('C')) AS t(x) WHERE x IN (SELECT city FROM \
             to_table(foo))",
            indoc!(
                r#"
                +---+
                | x |
                +---+
                | A |
                +---+
                "#
            ),
        ),
    ] {
        assert_data_eq(harness.sql(statement).await, expected).await;

        assert_eq!(
            harness.sql_state(statement).await.input_datasets,
            BTreeMap::from([(
                foo.get_id().clone(),
                QueryStateDataset {
                    alias: "foo".to_string(),
                    block_hash: head.clone(),
                }
            )]),
            "{statement}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_table_in_expression_subquery_is_not_hidden_by_cte_of_another_subquery() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    let head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,2000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // The CTE named `foo` exists only within the first subquery
    let statement = indoc!(
        r#"
        SELECT
            (WITH foo AS (SELECT 1 AS x) SELECT count(*) FROM foo) AS cte_rows,
            (SELECT count(*) FROM foo) AS dataset_rows
        "#
    );

    assert_data_eq(
        harness.sql(statement).await,
        indoc!(
            r#"
            +----------+--------------+
            | cte_rows | dataset_rows |
            +----------+--------------+
            | 1        | 2            |
            +----------+--------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness.sql_state(statement).await.input_datasets,
        BTreeMap::from([(
            foo.get_id().clone(),
            QueryStateDataset {
                alias: "foo".to_string(),
                block_hash: head,
            }
        )])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_inaccessible_dataset_is_reported_as_missing() {
    let harness = ToTableHarness::denying(&["secret"]);
    let secret = harness
        .create_dataset("secret", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    harness
        .write(
            &secret,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // The error must not reveal that an inaccessible dataset exists
    for (statement, missing) in [
        (
            "SELECT * FROM to_table(secret)",
            "SELECT * FROM to_table(missing)",
        ),
        (
            &format!("SELECT * FROM to_table(`{}`)", secret.get_id()) as &str,
            &format!(
                "SELECT * FROM to_table(`{}`)",
                ToTableHarness::dataset_id("missing")
            ) as &str,
        ),
    ] {
        let err = harness.sql_error(statement, QueryOptions::default()).await;
        let missing_err = harness.sql_error(missing, QueryOptions::default()).await;

        assert!(err.contains("not found"), "{err}");
        assert_eq!(
            ToTableHarness::mask_dataset_ref(&err, "secret"),
            ToTableHarness::mask_dataset_ref(&missing_err, "missing"),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_reads_only_input_datasets_of_the_query() {
    let harness = ToTableHarness::new();
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let bar = harness
        .create_dataset("bar", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    for dataset in [&foo, &bar] {
        harness
            .write(
                dataset,
                indoc!(
                    r#"
                    op,city,population
                    0,A,1000
                    "#
                ),
                schema,
            )
            .await;
    }

    // The dataset is referred to by the name the query gave it
    assert_data_eq(
        harness
            .sql_with(
                "SELECT city, population FROM to_table(renamed)",
                ToTableHarness::input_datasets(&[(&foo, "renamed")]),
            )
            .await,
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            +------+------------+
            "#
        ),
    )
    .await;

    // Accessible datasets that are not among the inputs are not visible
    let err = harness
        .sql_error(
            "SELECT * FROM to_table(bar)",
            ToTableHarness::input_datasets(&[(&foo, "foo")]),
        )
        .await;
    assert!(err.contains("not found"), "{err}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_inaccessible_input_dataset_is_reported_like_table_reference() {
    let harness = ToTableHarness::denying(&["secret"]);
    let secret = harness
        .create_dataset("secret", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    harness
        .write(
            &secret,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // Passing the dataset among the inputs of the query does not grant access to it: both a
    // plain table reference and `to_table()` fail as if the dataset did not exist
    let secret_id = secret.get_id();
    let missing_id = ToTableHarness::dataset_id("missing");
    for (statement, missing) in [
        (
            "SELECT * FROM secret".to_string(),
            "SELECT * FROM missing".to_string(),
        ),
        (
            "SELECT * FROM to_table(secret)".to_string(),
            "SELECT * FROM to_table(missing)".to_string(),
        ),
        (
            format!("SELECT * FROM to_table(`{secret_id}`)"),
            format!("SELECT * FROM to_table(`{missing_id}`)"),
        ),
    ] {
        let options = ToTableHarness::input_datasets(&[(&secret, "secret")]);

        assert_matches!(
            harness.sql_query_error(&statement, options.clone()).await,
            QueryError::BadQuery(_)
        );

        let err = harness.sql_error(&statement, options.clone()).await;
        let missing_err = harness.sql_error(&missing, options).await;

        assert!(err.contains("not found"), "{err}");
        assert_eq!(
            ToTableHarness::mask_dataset_ref(&err, "secret"),
            ToTableHarness::mask_dataset_ref(&missing_err, "missing"),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_in_session_reads_dataset_at_head() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,2000
                "#
            ),
            schema,
        )
        .await;
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                2,A,1000
                3,A,1100
                1,B,2000
                "#
            ),
            schema,
        )
        .await;

    let expected = indoc!(
        r#"
        +--------+------+------------+
        | offset | city | population |
        +--------+------+------------+
        | 3      | A    | 1100       |
        +--------+------+------------+
        "#
    );

    let foo_id = foo.get_id();
    for statement in [
        "SELECT offset, city, population FROM to_table(foo)".to_string(),
        format!("SELECT offset, city, population FROM to_table(`{foo_id}`)"),
    ] {
        assert_data_eq(harness.session_sql(&statement).await, expected).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_in_session_inaccessible_dataset_is_reported_as_missing() {
    let harness = ToTableHarness::denying(&["secret"]);
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let secret = harness
        .create_dataset("secret", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    for dataset in [&foo, &secret] {
        harness
            .write(
                dataset,
                indoc!(
                    r#"
                    op,city,population
                    0,A,1000
                    "#
                ),
                schema,
            )
            .await;
    }

    // Readable datasets stay reachable in the same session
    assert_data_eq(
        harness
            .session_sql("SELECT city, population FROM to_table(foo)")
            .await,
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            +------+------------+
            "#
        ),
    )
    .await;

    let secret_id = secret.get_id();
    let missing_id = ToTableHarness::dataset_id("missing");
    for (statement, missing) in [
        (
            "SELECT * FROM secret".to_string(),
            "SELECT * FROM missing".to_string(),
        ),
        (
            "SELECT * FROM to_table(secret)".to_string(),
            "SELECT * FROM to_table(missing)".to_string(),
        ),
        (
            format!("SELECT * FROM to_table(`{secret_id}`)"),
            format!("SELECT * FROM to_table(`{missing_id}`)"),
        ),
    ] {
        let err = harness.session_sql_error(&statement).await;
        let missing_err = harness.session_sql_error(&missing).await;

        assert!(err.contains("not found"), "{err}");
        assert_eq!(
            ToTableHarness::mask_dataset_ref(&err, "secret"),
            ToTableHarness::mask_dataset_ref(&missing_err, "missing"),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_dataset_without_data() {
    let harness = ToTableHarness::new();

    // No schema yet
    harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    // Schema, but no records
    let bar = harness
        .create_dataset("bar", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    harness
        .write(
            &bar,
            "op,city,population\n",
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    for name in ["foo", "bar"] {
        assert_data_eq(
            harness
                .sql(&format!("SELECT COUNT(*) AS num FROM to_table({name})"))
                .await,
            indoc!(
                r#"
                +-----+
                | num |
                +-----+
                | 0   |
                +-----+
                "#
            ),
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_joined_with_its_changelog_sees_same_block() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    let first_head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            schema,
        )
        .await;
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                1,A,1000
                0,A,2000
                "#
            ),
            schema,
        )
        .await;

    let statement = indoc!(
        r#"
        SELECT log.offset, log.op, log.population, state.population AS current
        FROM foo AS log
        JOIN to_table(foo) AS state ON log.city = state.city
        ORDER BY log.offset
        "#
    );

    assert_data_eq(
        harness.sql_as_of(statement, &foo, first_head).await,
        indoc!(
            r#"
            +--------+----+------------+---------+
            | offset | op | population | current |
            +--------+----+------------+---------+
            | 0      | 0  | 1000       | 1000    |
            +--------+----+------------+---------+
            "#
        ),
    )
    .await;

    assert_data_eq(
        harness.sql(statement).await,
        indoc!(
            r#"
            +--------+----+------------+---------+
            | offset | op | population | current |
            +--------+----+------------+---------+
            | 0      | 0  | 1000       | 2000    |
            | 1      | 1  | 1000       | 2000    |
            | 2      | 0  | 2000       | 2000    |
            +--------+----+------------+---------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_requires_primary_key() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
            None,
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1000
                "#
            ),
            "city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_matches!(
        harness
            .sql_query_error("SELECT * FROM to_table(foo)", QueryOptions::default())
            .await,
        QueryError::Internal(e)
            if ToTableHarness::error_chain(&e).contains("'foo' has no primary keys")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_of_dataset_named_differently_as_table() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                2,A,1000
                3,A,2000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    let id = ToTableHarness::dataset_id("foo");
    let expected = indoc!(
        r#"
        +-----+---------+
        | log | current |
        +-----+---------+
        | 0   | 2000    |
        | 1   | 2000    |
        | 2   | 2000    |
        +-----+---------+
        "#
    );

    for statement in [
        format!(
            "SELECT log.offset AS log, state.population AS current FROM foo AS log JOIN \
             to_table(`{id}`) AS state ON log.city = state.city ORDER BY log.offset"
        ),
        format!(
            "SELECT log.offset AS log, state.population AS current FROM to_table(foo) AS state \
             JOIN `{id}` AS log ON log.city = state.city ORDER BY log.offset"
        ),
    ] {
        assert_data_eq(harness.sql(&statement).await, expected).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ToTableHarness {
    temp_dir: tempfile::TempDir,
    query_svc: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl ToTableHarness {
    fn new() -> Self {
        Self::denying(&[])
    }

    /// Datasets of the specified names cannot be read
    fn denying(names: &[&str]) -> Self {
        let denied_ids = names
            .iter()
            .map(|name| Self::dataset_id(name))
            .collect::<HashSet<_>>();

        Self::with_permission_checker(MockDatasetPermissionChecker::denying_ids(denied_ids))
    }

    fn with_permission_checker(dataset_permission_checker: MockDatasetPermissionChecker) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_catalog = helpers::create_base_catalog_with_local_workspace(
            temp_dir.path(),
            dataset_permission_checker,
        );

        let catalog = dill::CatalogBuilder::new_chained(&base_catalog)
            .add::<QueryServiceImpl>()
            .build();

        Self {
            temp_dir,
            query_svc: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
        }
    }

    /// Datasets get IDs derived from their names, so they can be referred to before creation
    fn dataset_id(name: &str) -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(name.as_bytes())
    }

    fn changelog_stream(primary_key: &[&str]) -> odf::metadata::MergeStrategy {
        odf::metadata::MergeStrategy::ChangelogStream(odf::metadata::MergeStrategyChangelogStream {
            primary_key: primary_key.iter().map(ToString::to_string).collect(),
        })
    }

    /// Dataset with a polling source using the specified merge strategy
    async fn create_dataset(
        &self,
        name: &str,
        merge: odf::metadata::MergeStrategy,
        vocab: Option<odf::metadata::SetVocab>,
    ) -> ResolvedDataset {
        self.create_dataset_with_source(
            name,
            MetadataFactory::set_polling_source()
                .merge(merge)
                .build()
                .into(),
            vocab,
        )
        .await
    }

    async fn create_dataset_with_source(
        &self,
        name: &str,
        source: odf::MetadataEvent,
        vocab: Option<odf::metadata::SetVocab>,
    ) -> ResolvedDataset {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name));

        let mut snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root);
        if let Some(vocab) = vocab {
            snapshot = snapshot.push_event(vocab);
        }
        let snapshot = snapshot.push_event(source).build();

        let stored = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            Self::dataset_id(name),
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        ResolvedDataset::from_stored(&stored, &alias)
    }

    /// Ingests CSV data through the data writer and returns the new head
    async fn write(&self, dataset: &ResolvedDataset, csv: &str, schema: &str) -> odf::Multihash {
        // Natural order of records is only preserved in a single partition
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        let data_path = self.temp_dir.path().join("data.csv");
        std::fs::write(&data_path, csv).unwrap();

        let df = ReaderCsv::new(
            ctx.clone(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
                schema: Some(odf::utils::schema::parse::parse_ddl_to_odf_schema(schema).unwrap()),
                ..Default::default()
            },
            &odf::schema::ToArrowSettings::default(),
        )
        .unwrap()
        .read(&data_path)
        .await
        .unwrap();

        let mut writer = DataWriterDataFusion::from_metadata_chain(
            ctx,
            dataset.clone(),
            &odf::BlockRef::Head,
            None,
        )
        .await
        .unwrap();

        let now = self.system_time_source.now();
        let write_result = writer
            .write(
                Some(df),
                WriteDataOpts {
                    system_time: now,
                    source_event_time: now,
                    new_watermark: None,
                    new_source_state: None,
                    data_staging_path: self.temp_dir.path().join(".temp-data.parquet"),
                },
            )
            .await
            .unwrap();

        dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &write_result.new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(Some(&write_result.old_head)),
                },
            )
            .await
            .unwrap();

        write_result.new_head
    }

    /// Messages of the error and all its sources, as wrapping layers hide the cause
    fn error_chain(err: &dyn std::error::Error) -> String {
        std::iter::successors(Some(err), |e| e.source())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": ")
    }

    /// Replaces the name and the ID of the dataset in an error message, so that errors about
    /// different datasets can be compared
    fn mask_dataset_ref(err: &str, name: &str) -> String {
        err.replace(&Self::dataset_id(name).to_string(), "<ref>")
            .replace(name, "<ref>")
    }

    async fn sql(&self, statement: &str) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(statement, QueryOptions::default())
            .await
            .unwrap()
            .df
    }

    async fn sql_state(&self, statement: &str) -> QueryState {
        self.query_svc
            .sql_statement(statement, QueryOptions::default())
            .await
            .unwrap()
            .state
    }

    async fn sql_query_error(&self, statement: &str, options: QueryOptions) -> QueryError {
        let Err(err) = self.query_svc.sql_statement(statement, options).await else {
            panic!("Expected statement to fail: {statement}");
        };
        err
    }

    /// Error messages of a failing statement, including all their sources
    async fn sql_error(&self, statement: &str, options: QueryOptions) -> String {
        Self::error_chain(&self.sql_query_error(statement, options).await)
    }

    /// Runs the statement in a session of the kind `FlightSQL` and the SQL shell use
    async fn session_sql(&self, statement: &str) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .create_session()
            .await
            .unwrap()
            .sql(statement)
            .await
            .unwrap()
            .into()
    }

    /// Error messages of a statement failing in a session, including all their sources
    async fn session_sql_error(&self, statement: &str) -> String {
        let session = self.query_svc.create_session().await.unwrap();
        let Err(err) = session.sql(statement).await else {
            panic!("Expected statement to fail: {statement}");
        };
        Self::error_chain(&err)
    }

    /// Options that restrict the query to the specified datasets under the specified names
    fn input_datasets(datasets: &[(&ResolvedDataset, &str)]) -> QueryOptions {
        QueryOptions {
            input_datasets: Some(
                datasets
                    .iter()
                    .map(|(dataset, alias)| {
                        (
                            dataset.get_id().clone(),
                            QueryOptionsDataset {
                                alias: (*alias).to_string(),
                                ..Default::default()
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    async fn sql_with(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(statement, options)
            .await
            .unwrap()
            .df
    }

    /// Runs the statement with the dataset pinned to the specified block
    async fn sql_as_of(
        &self,
        statement: &str,
        dataset: &ResolvedDataset,
        block_hash: odf::Multihash,
    ) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(
                statement,
                QueryOptions {
                    input_datasets: Some(BTreeMap::from([(
                        dataset.get_id().clone(),
                        QueryOptionsDataset {
                            alias: dataset.get_alias().dataset_name.to_string(),
                            block_hash: Some(block_hash),
                            ..Default::default()
                        },
                    )])),
                },
            )
            .await
            .unwrap()
            .df
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
