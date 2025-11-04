// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::error::DataFusionError as DFError;
use datafusion::prelude::*;
use indoc::{formatdoc, indoc};
use kamu_core::{GetDataResponse, MockQueryService, QueryService};
use pretty_assertions::assert_matches;

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dataset_unauthorized() {
    let ctx = create_session_context();

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_empty_dataset() {
    let ctx = create_session_context();

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_function_returns_correct_results() {
    let ctx = create_session_context();

    for table_arg in ["`kamu/table`", "\"kamu/table\""] {
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
    kamu_datafusion_udf::ToTableUdtf::register(&ctx, create_mock_query_svc());
    ctx
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_mock_query_svc() -> Arc<dyn QueryService> {
    let mut mock_query_svc = MockQueryService::new();

    mock_query_svc
        .expect_get_changelog_projection()
        .returning(|dataset_ref, _options| {
            let Some(dataset_alias) = dataset_ref.alias() else {
                unreachable!()
            };

            let has_data = match dataset_alias.dataset_name.as_str() {
                "table" => true,
                "table-no-schema" => false,
                "unauthorized-table" => {
                    return Err(
                        odf::AccessError::Unauthorized("Dataset inaccessible".into()).into(),
                    );
                }
                _ => {
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
                dataset_handle: odf::DatasetHandle::new(
                    odf::DatasetID::new_generated_ed25519().1,
                    dataset_alias.clone(),
                    odf::DatasetKind::Root,
                ),
                block_hash: odf::Multihash::from_digest_sha3_256(b"head"),
            })
        });

    Arc::new(mock_query_svc)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
