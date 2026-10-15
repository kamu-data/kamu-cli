// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::*;
use indoc::indoc;
use odf_metadata::DatasetVocabulary;
use opendatafabric_data_utils::data::{DataFrameExt, changelog};
use opendatafabric_data_utils::testing::assert_data_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const LEDGER: &str = "offset BIGINT, op INT, city VARCHAR, population BIGINT";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_project_corrections_and_retractions() {
    let harness = ChangelogHarness::new();

    // The example from the `project` docs
    let ledger = harness
        .ledger(
            LEDGER,
            &[
                "(0, 0, 'a', 1000), (1, 0, 'b', 2000), (2, 0, 'c', 3000), (3, 2, 'b', 2000), (4, \
                 3, 'b', 2500), (5, 2, 'a', 1000), (6, 3, 'a', 1500), (7, 1, 'a', 1500)",
            ],
        )
        .await;

    assert_data_eq(
        harness.project(ledger, &["city"]),
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 2      | 0  | c    | 3000       |
            | 4      | 3  | b    | 2500       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_project_retracted_key_added_again() {
    let harness = ChangelogHarness::new();

    let ledger = harness
        .ledger(
            LEDGER,
            &[
                "(0, 0, 'a', 1000), (1, 1, 'a', 1000), (2, 0, 'a', 1100), (3, 0, 'b', 2000), (4, \
                 1, 'b', 2000), (5, 0, 'b', 2100), (6, 1, 'b', 2100)",
            ],
        )
        .await;

    assert_data_eq(
        harness.project(ledger, &["city"]),
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 2      | 0  | a    | 1100       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_project_changes_of_key_spread_across_partitions_in_any_order() {
    let harness = ChangelogHarness::new();

    // Neither the partitions nor the rows within them follow the offsets, as when a
    // ledger is read from several files
    let ledger = harness
        .ledger(
            LEDGER,
            &[
                "(7, 3, 'a', 1300), (1, 0, 'b', 2000), (4, 2, 'a', 1100)",
                "(0, 0, 'a', 1000), (6, 2, 'a', 1200), (8, 1, 'b', 2000)",
                "(5, 3, 'a', 1200), (2, 2, 'a', 1000), (3, 3, 'a', 1100), (9, 0, 'c', 3000), (10, \
                 1, 'c', 3000)",
                "(11, 0, 'c', 3100), (12, 2, 'c', 3100), (13, 3, 'c', 3200)",
            ],
        )
        .await;

    assert_data_eq(
        harness.project(ledger, &["city"]),
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 7      | 3  | a    | 1300       |
            | 13     | 3  | c    | 3200       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_project_op_column_of_unsigned_byte_type() {
    let harness = ChangelogHarness::new();

    let ledger = harness
        .ledger(
            "offset BIGINT, op TINYINT UNSIGNED, city VARCHAR, population BIGINT",
            &[
                "(0, 0, 'a', 1000), (1, 0, 'b', 2000), (2, 2, 'a', 1000), (3, 3, 'a', 1100), (4, \
                 1, 'b', 2000)",
            ],
        )
        .await;

    assert_data_eq(
        harness.project(ledger, &["city"]),
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 3      | 3  | a    | 1100       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_project_composite_primary_key() {
    let harness = ChangelogHarness::new();

    let ledger = harness
        .ledger(
            "offset BIGINT, op INT, city VARCHAR, year INT, population BIGINT",
            &[
                "(0, 0, 'a', 2020, 1000), (1, 0, 'a', 2021, 1100), (2, 0, 'b', 2020, 2000), (3, \
                 2, 'a', 2020, 1000), (4, 3, 'a', 2020, 1050), (5, 1, 'b', 2020, 2000), (6, 0, \
                 'b', 2021, 2100)",
            ],
        )
        .await;

    assert_data_eq(
        harness.project(ledger, &["city", "year"]),
        indoc!(
            r#"
            +--------+----+------+------+------------+
            | offset | op | city | year | population |
            +--------+----+------+------+------------+
            | 1      | 0  | a    | 2021 | 1100       |
            | 4      | 3  | a    | 2020 | 1050       |
            | 6      | 0  | b    | 2021 | 2100       |
            +--------+----+------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ChangelogHarness {
    ctx: SessionContext,
}

impl ChangelogHarness {
    fn new() -> Self {
        Self {
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4)),
        }
    }

    /// A ledger with the specified `name TYPE` columns, read from one partition per
    /// entry of SQL `VALUES` rows
    async fn ledger(&self, columns: &str, partitions: &[&str]) -> DataFrameExt {
        let projection = columns
            .split(',')
            .enumerate()
            .map(|(i, column)| {
                let (name, data_type) = column.trim().split_once(' ').unwrap();
                format!("CAST(column{} AS {data_type}) AS {name}", i + 1)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let mut ledger: Option<DataFrame> = None;
        for rows in partitions {
            let partition = self
                .ctx
                .sql(&format!("SELECT {projection} FROM (VALUES {rows})"))
                .await
                .unwrap();

            ledger = Some(match ledger {
                None => partition,
                Some(ledger) => ledger.union(partition).unwrap(),
            });
        }

        ledger.unwrap().into()
    }

    /// Projected state ordered by offset
    fn project(&self, ledger: DataFrameExt, primary_key: &[&str]) -> DataFrameExt {
        let primary_key: Vec<String> = primary_key.iter().map(ToString::to_string).collect();

        changelog::project(ledger, &primary_key, &DatasetVocabulary::default())
            .unwrap()
            .sort(vec![col("offset").sort(true, false)])
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
