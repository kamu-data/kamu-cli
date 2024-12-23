// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use datafusion::arrow::array::StringArray;
use datafusion::prelude::{SessionContext, *};
use indoc::indoc;
use kamu_ingest_datafusion::*;

use super::test_reader_common;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_shapefile_with_schema() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader_success(
        ReaderEsriShapefile::new(
            SessionContext::new(),
            odf::metadata::ReadStepEsriShapefile {
                schema: Some(vec![
                    "iso string not null".to_string(),
                    "name_0 string not null".to_string(),
                    "name_1 string not null".to_string(),
                ]),
                sub_path: None,
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        |path| async {
            std::fs::copy("tests/data/ukraine.zip", path).unwrap();
        },
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED BYTE_ARRAY iso (STRING);
              REQUIRED BYTE_ARRAY name_0 (STRING);
              REQUIRED BYTE_ARRAY name_1 (STRING);
            }
            "#
        ),
        indoc!(
            r#"
            +-----+---------+------------------+
            | iso | name_0  | name_1           |
            +-----+---------+------------------+
            | UKR | Ukraine | Cherkasy         |
            | UKR | Ukraine | Chernihiv        |
            | UKR | Ukraine | Chernivtsi       |
            | UKR | Ukraine | Crimea           |
            | UKR | Ukraine | Dnipropetrovs'k  |
            | UKR | Ukraine | Donets'k         |
            | UKR | Ukraine | Ivano-Frankivs'k |
            | UKR | Ukraine | Kharkiv          |
            | UKR | Ukraine | Kherson          |
            | UKR | Ukraine | Khmel'nyts'kyy   |
            | UKR | Ukraine | Kiev City        |
            | UKR | Ukraine | Kiev             |
            | UKR | Ukraine | Kirovohrad       |
            | UKR | Ukraine | L'viv            |
            | UKR | Ukraine | Luhans'k         |
            | UKR | Ukraine | Mykolayiv        |
            | UKR | Ukraine | Odessa           |
            | UKR | Ukraine | Poltava          |
            | UKR | Ukraine | Rivne            |
            | UKR | Ukraine | Sevastopol'      |
            | UKR | Ukraine | Sumy             |
            | UKR | Ukraine | Ternopil'        |
            | UKR | Ukraine | Transcarpathia   |
            | UKR | Ukraine | Vinnytsya        |
            | UKR | Ukraine | Volyn            |
            | UKR | Ukraine | Zaporizhzhya     |
            | UKR | Ukraine | Zhytomyr         |
            +-----+---------+------------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_shapefile_infer_schema() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader(
        ReaderEsriShapefile::new(
            SessionContext::new(),
            odf::metadata::ReadStepEsriShapefile {
                schema: None,
                sub_path: None,
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        |path| async {
            std::fs::copy("tests/data/ukraine.zip", path).unwrap();
        },
        |res| async {
            let df = res.unwrap();
            odf::utils::testing::assert_schema_eq(
                df.schema(),
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT32 cca_1 (UNKNOWN);
                      OPTIONAL DOUBLE ccn_1;
                      OPTIONAL BYTE_ARRAY engtype_1 (STRING);
                      OPTIONAL BYTE_ARRAY geometry (STRING);
                      OPTIONAL BYTE_ARRAY hasc_1 (STRING);
                      OPTIONAL DOUBLE id_0;
                      OPTIONAL DOUBLE id_1;
                      OPTIONAL BYTE_ARRAY iso (STRING);
                      OPTIONAL BYTE_ARRAY name_0 (STRING);
                      OPTIONAL BYTE_ARRAY name_1 (STRING);
                      OPTIONAL INT32 nl_name_1 (UNKNOWN);
                      OPTIONAL BYTE_ARRAY type_1 (STRING);
                      OPTIONAL BYTE_ARRAY varname_1 (STRING);
                    }
                    "#
                ),
            );
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_shapefile_with_subpath_exists() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader(
        ReaderEsriShapefile::new(
            SessionContext::new(),
            odf::metadata::ReadStepEsriShapefile {
                schema: None,
                sub_path: Some("gg870xt4706.shp".to_string()),
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        |path| async {
            std::fs::copy("tests/data/ukraine.zip", path).unwrap();
        },
        |res| async move {
            assert_matches!(res, Ok(_));
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_shapefile_with_subpath_missing() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader(
        ReaderEsriShapefile::new(
            SessionContext::new(),
            odf::metadata::ReadStepEsriShapefile {
                schema: None,
                sub_path: Some("invalid.shp".to_string()),
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        |path| async {
            std::fs::copy("tests/data/ukraine.zip", path).unwrap();
        },
        |res| async move {
            assert_matches!(res, Err(_));
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_shapefile_geom() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader(
        ReaderEsriShapefile::new(
            SessionContext::new(),
        odf::metadata::ReadStepEsriShapefile {
                schema: Some(vec![
                    "geometry string not null".to_string(),
                    "name_1 string not null".to_string(),
                ]),
                sub_path: None,
            },
            temp_dir.path().join("reader-tmp")
        )
        .await
        .unwrap(),
        |path| async {
            std::fs::copy("tests/data/ukraine.zip", path).unwrap();
        },
        |df| async {
            let df = df
                .unwrap()
                .filter(col("name_1").eq(lit("Kiev City")))
                .unwrap()
                .repartition(Partitioning::RoundRobinBatch(1))
                .unwrap();

            let batches = df.collect().await.unwrap();
            let batch = &batches[0];
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.num_rows(), 1);

            let geom = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);

            let mut geojson: serde_json::Value = serde_json::from_str(geom).unwrap();
            geojson["coordinates"][0][0]
                .as_array_mut()
                .unwrap()
                .truncate(2);

            assert_eq!(
                geojson,
                serde_json::json!({
                    "type": "MultiPolygon",
                    "coordinates": [[[
                        [30.466304779052763, 50.58700942993181], [30.466583251953324, 50.58112716674821]
                    ]]],
                })
            );
        },
    )
    .await;
}
