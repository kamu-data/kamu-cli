// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, LazyLock};

use datafusion::error::DataFusionError as DFError;
use datafusion::prelude::*;
use indoc::{formatdoc, indoc};
use kamu_core::{GetDataResponse, MockQueryDatasetDataUseCase, QueryDatasetDataUseCase};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static REGULAR_TABLE_DATASET_ID: LazyLock<odf::DatasetID> =
    LazyLock::new(|| odf::DatasetID::new_seeded_ed25519(b"REGULAR_TABLE_DATASET_ID"));
static NO_SCHEMA_TABLE_DATASET_ID: LazyLock<odf::DatasetID> =
    LazyLock::new(|| odf::DatasetID::new_seeded_ed25519(b"NO_SCHEMA_TABLE_DATASET_ID"));
static UNAUTHORIZED_TABLE_DATASET_ID: LazyLock<odf::DatasetID> =
    LazyLock::new(|| odf::DatasetID::new_seeded_ed25519(b"UNAUTHORIZED_TABLE_DATASET_ID"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_wrong_arguments() {
    let ctx = create_session_context();

    for arguments in [
        "",                           // no arguments
        "`kamu/unknown-table`, path", // more than one argument
    ] {
        assert_matches!(
            ctx.sql(&formatdoc!(
                "
                SELECT *
                FROM to_table({arguments})
                "
            ))
            .await,
            Err(DFError::Plan(e))
                if e == "to_table() accepts only one argument: dataset_ref",
            "Arguments: {}", arguments
        );
    }

    for arguments in [
        "1",                    // number
        "'kamu/unknown-table'", // string literal not a table reference
        "kamu/unknown-table",   // identifiers w/ division operator
    ] {
        assert_matches!(
            ctx.sql(&formatdoc!(
                "
                SELECT *
                FROM to_table({arguments})
                "
            ))
            .await,
            Err(DFError::Plan(e))
                if e == "to_table(): dataset_ref must be a table reference",
            "Arguments: {}", arguments
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dataset_not_found() {
    let ctx = create_session_context();

    // Handle
    assert_matches!(
        ctx.sql(indoc!(
            "
            SELECT *
            FROM to_table(`kamu/unknown-table`)
            "
        ))
        .await,
        Err(DFError::External(e))
            if e.to_string() == "Dataset not found: kamu/unknown-table"
    );
    // ID
    let not_found_table_dataset_id = odf::DatasetID::new_generated_ed25519().1;
    assert_matches!(
        ctx.sql(&formatdoc!(
            "
            SELECT *
            FROM to_table(`{not_found_table_dataset_id}`)
            ",
        ))
        .await,
        Err(DFError::External(e))
            if e.to_string() == format!("Dataset not found: {not_found_table_dataset_id}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dataset_unauthorized() {
    let ctx = create_session_context();

    // Handle
    assert_matches!(
        ctx.sql(indoc!(
            "
            SELECT *
            FROM to_table(`kamu/unauthorized-table`)
            "
        ))
        .await,
        Err(DFError::External(e))
            if e.to_string() == "Dataset not found: kamu/unauthorized-table"
    );

    // ID
    assert_matches!(
        ctx.sql(&formatdoc!(
            "
            SELECT *
            FROM to_table(`{dataset_id}`)
            ",
            dataset_id = *UNAUTHORIZED_TABLE_DATASET_ID
        ))
        .await,
        Err(DFError::External(e))
            if e.to_string() == format!("Dataset not found: {}", *UNAUTHORIZED_TABLE_DATASET_ID)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_empty_dataset() {
    let ctx = create_session_context();

    // Handle
    odf::utils::testing::assert_data_eq(
        ctx.sql(&formatdoc!(
            "
            SELECT *
            FROM to_table(`kamu/table-no-schema`)
            "
        ))
        .await
        .unwrap()
        .into(),
        indoc!(
            "
            ++
            ++
            "
        ),
    )
    .await;

    // ID
    odf::utils::testing::assert_data_eq(
        ctx.sql(&formatdoc!(
            "
            SELECT *
            FROM to_table(`{dataset_id}`)
            ",
            dataset_id = *NO_SCHEMA_TABLE_DATASET_ID
        ))
        .await
        .unwrap()
        .into(),
        indoc!(
            "
            ++
            ++
            "
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_function_returns_correct_results() {
    let ctx = create_session_context();

    for table_arg in [
        // Handle
        "`kamu/table`",
        "\"kamu/table\"",
        // ID
        &format!("`{}`", *REGULAR_TABLE_DATASET_ID),
        &format!("\"{}\"", *REGULAR_TABLE_DATASET_ID),
    ] {
        // +--------+----+------+------------+
        // | offset | op | city | population |
        // +--------+----+------+------------+
        // | 0      | +A | a    | 1000       |
        // | 1      | +A | b    | 2000       |
        // | 2      | +A | c    | 3000       |
        // | 3      | -C | b    | 2000       |
        // | 4      | +C | b    | 2500       |
        // | 5      | -C | a    | 1000       |
        // | 6      | +C | a    | 1500       |
        // | 7      | -R | a    | 1500       |
        // +--------+----+------+------------+
        odf::utils::testing::assert_data_eq(
            ctx.sql(&formatdoc!(
                "
                SELECT *
                FROM to_table({table_arg})
                ORDER BY offset
                "
            ))
            .await
            .unwrap()
            .into(),
            indoc!(
                "
                +--------+----+------+------------+
                | offset | op | city | population |
                +--------+----+------+------------+
                | 2      | 0  | c    | 3000       |
                | 4      | 3  | b    | 2500       |
                +--------+----+------+------------+
                "
            ),
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_session_context() -> SessionContext {
    let ctx = SessionContext::new();
    kamu_datafusion_udf::ToTableUdtf::register(&ctx, create_mock_query_dataset_data_use_case());
    ctx
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_mock_query_dataset_data_use_case() -> Arc<dyn QueryDatasetDataUseCase> {
    let mut mock_query_dataset_data_use_case = MockQueryDatasetDataUseCase::new();

    mock_query_dataset_data_use_case
        .expect_get_changelog_projection()
        .returning(|dataset_ref, _options| {
            enum MockTable {
                Regular,
                NoSchema,
                Unauthorized,
                NotFound,
            }

            let dummy_table = match dataset_ref {
                odf::DatasetRef::ID(id) => {
                    if *id == *REGULAR_TABLE_DATASET_ID {
                        MockTable::Regular
                    } else if *id == *NO_SCHEMA_TABLE_DATASET_ID {
                        MockTable::NoSchema
                    } else if *id == *UNAUTHORIZED_TABLE_DATASET_ID {
                        MockTable::Unauthorized
                    } else {
                        MockTable::NotFound
                    }
                }
                odf::DatasetRef::Alias(alias) => match alias.dataset_name.as_str() {
                    "table" => MockTable::Regular,
                    "table-no-schema" => MockTable::NoSchema,
                    "unauthorized-table" => MockTable::Unauthorized,
                    _ => MockTable::NotFound,
                },
                odf::DatasetRef::Handle(_) => {
                    unreachable!()
                }
            };

            let has_data = match dummy_table {
                MockTable::Regular => true,
                MockTable::NoSchema => false,
                MockTable::Unauthorized => {
                    return Err(
                        odf::AccessError::Unauthorized("Dataset inaccessible".into()).into(),
                    );
                }
                MockTable::NotFound => {
                    return Err(odf::DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }
                    .into());
                }
            };

            Ok(GetDataResponse {
                df: has_data.then(|| {
                    // +--------+----+------+------------+
                    // | offset | op | city | population |
                    // +--------+----+------+------------+
                    // | 0      | +A | a    | 1000       |
                    // | 1      | +A | b    | 2000       |
                    // | 2      | +A | c    | 3000       |
                    // | 3      | -C | b    | 2000       |
                    // | 4      | +C | b    | 2500       |
                    // | 5      | -C | a    | 1000       |
                    // | 6      | +C | a    | 1500       |
                    // | 7      | -R | a    | 1500       |
                    // +--------+----+------+------------+
                    let df = dataframe!(
                        "offset" => [0, 1, 2, 3, 4, 5, 6, 7],
                        "op" => [0, 0, 0, 2, 3, 2, 3, 1],
                        "city" => ["a", "b", "c", "b", "b", "a", "a", "a"],
                        "population" => [1000, 2000, 3000, 2000, 2500, 1000, 1500, 1500],
                    )
                    .unwrap();

                    odf::utils::data::changelog::project(
                        df.into(),
                        &["city".to_string()],
                        &Default::default(),
                    )
                    .unwrap()
                }),
                source: kamu_datasets::ResolvedDataset::new(
                    Arc::new(odf::dataset::MockDataset::new()),
                    odf::metadata::testing::handle(&"foo", &"bar", odf::DatasetKind::Root),
                ),
                block_hash: odf::Multihash::from_digest_sha3_256(b"head"),
            })
        });

    Arc::new(mock_query_dataset_data_use_case)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
