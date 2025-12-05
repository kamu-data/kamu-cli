// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::prelude::*;
use file_utils::MediaType;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryAccountRepository};
use kamu_accounts_services::{
    AccountQuotaServiceImpl,
    AccountServiceImpl,
    QuotaCheckerStorageImpl,
};
use kamu_datasets::*;
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::DatasetStatisticsServiceImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_push_url_stream() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..odf::metadata::ReadStepCsv::default()
                })
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1: Push from URL
    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target.clone(),
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts::default(),
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-01T00:00:00+00:00".to_string())
    );

    // Round 2: Push from Stream
    let data = std::io::Cursor::new(indoc!(
        "
        date,city,population
        2020-01-01,B,2000
        2020-01-01,C,3000
        2021-01-01,C,4000
        "
    ));

    harness
        .ingest_from(
            target,
            None,
            DataSource::Stream(Box::new(data)),
            PushIngestOpts::default(),
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | date                 | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2050-01-01T12:00:00Z | 2021-01-01T00:00:00Z | C    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_push_media_type_override() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..Default::default()
                })
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Push CSV conversion
    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            2020-01-01,A,1000
            "
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target.clone(),
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                media_type: Some(MediaType::CSV.to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    // Push NDJSON native
    let src_path = harness.temp_dir.path().join("data.json");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"date": "2020-01-01", "city": "B", "population": 2000}
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target.clone(),
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                media_type: Some(MediaType::NDJSON.to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    // Push JSON conversion
    let src_path = harness.temp_dir.path().join("data.json");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            [
                {"date": "2020-01-01", "city": "C", "population": 3000}
            ]
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                media_type: Some(MediaType::JSON.to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_push_schema_stability() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["event_time TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..odf::metadata::ReadStepCsv::default()
                })
                .merge(odf::metadata::MergeStrategyAppend {})
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1: Push from URL
    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            event_time,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts::default(),
        )
        .await
        .unwrap();

    // This should be a lossless conversion of ODF schema in metadata chain to arrow
    let schema_in_metadata_as_arrow = Arc::new(
        data_helper
            .get_last_set_data_schema_block()
            .await
            .event
            .schema_as_arrow(&odf::metadata::ToArrowSettings::default())
            .unwrap(),
    );

    // This is schema that datafusion interprets when reading the output parquet
    // file. It may differ from the schema in metadata due to view encoding
    // optimization auto-applied by datafusion
    let schema_on_parquet_read = data_helper.get_last_data().await.schema().inner().clone();

    odf::utils::testing::assert_arrow_schema_eq(
        &schema_in_metadata_as_arrow,
        json!({
            "fields": [{
                "name": "offset",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "op",
                "data_type": "Int32",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "system_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "event_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "city",
                // NOTE: We strip the encoding details in SetDataSchema, leaving only logical types
                "data_type": "Utf8",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": true,
            }, {
                "name": "population",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": true,
            }],
            "metadata": {},
        }),
    );

    odf::utils::testing::assert_arrow_schema_eq(
        &schema_on_parquet_read,
        json!({
            "fields": [{
                "name": "offset",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "op",
                "data_type": "Int32",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "system_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "event_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "city",
                // NOTE: Since Datafusion 49 view encoding is used by default on Parquet read and is retained here
                "data_type": "Utf8View",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": true,
            }, {
                "name": "population",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": true,
            }],
            "metadata": {},
        }),
    );

    // This schema is captured earlier with:
    // - kamu-cli = 'branch/breaking-changes'
    // - datafusion = 33
    //
    // To refresh use:
    // println!("{}", hex::encode(set_data_schema.schema.as_slice()));

    let schema_prev = odf::metadata::SetDataSchema {
        raw_arrow_schema: Some(hex::decode(
            "0c00000008000800000004000800000004000000060000004401000004010000ac0000006c0000\
            003c00000004000000e4feffff10000000180000000000010214000000d4feffff4000000000000\
            001000000000a000000706f70756c6174696f6e000018ffffff180000000c000000000001051000\
            000000000000040004000400000004000000636974790000000044ffffff140000000c000000000\
            0010a1c00000000000000c4ffffff080000000000010003000000555443000a0000006576656e74\
            5f74696d650000bcffffff1c0000000c0000000000000a240000000000000008000c000a0004000\
            8000000080000000000010003000000555443000b00000073797374656d5f74696d650010001400\
            100000000f000400000008001000000010000000180000000000000214000000c4ffffff2000000\
            00000000100000000020000006f7000001000140010000e000f0004000000080010000000180000\
            0020000000000001021c00000008000c0004000b000800000040000000000000010000000006000\
            0006f66667365740000").unwrap()),
        schema: None,
    }.schema_as_arrow(&odf::metadata::ToArrowSettings::default()).map(Arc::new).unwrap();

    kamu_ingest_datafusion::DataWriterDataFusion::validate_schema_compatible(
        &schema_prev,
        &schema_in_metadata_as_arrow,
    )
    .expect(
        "Schema drift detected! Schema produced by the current kamu/datafusion version is not \
         equivalent to the schema produced previously. This will result in writer errors on \
         existing datasets. You'll need to investigate how exactly the schema representation \
         changed and whether equivalence test needs to be relaxed.",
    );

    kamu_ingest_datafusion::DataWriterDataFusion::validate_schema_compatible(
        &schema_prev,
        &schema_on_parquet_read,
    )
    .expect(
        "Schema drift detected! Schema produced by the current kamu/datafusion version is not \
         equivalent to the schema produced previously. This will result in writer errors on \
         existing datasets. You'll need to investigate how exactly the schema representation \
         changed and whether equivalence test needs to be relaxed.",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_inference_automatic_coercion_of_event_time_from_string() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            // Note: not setting schema or adding a preprocess step to trigger inference
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson::default())
                .merge(odf::metadata::MergeStrategyAppend {})
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    let src_path = harness.temp_dir.path().join("data.ndjson");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"event_time": "2020-01-02T01:02:03.123456789Z", "foo": "bar"}
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                schema_inference: SchemaInferenceOpts {
                    coerce_event_time_column_type: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY foo (STRING);
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+--------------------------+-----+
                | offset | op | system_time          | event_time               | foo |
                +--------+----+----------------------+--------------------------+-----+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T01:02:03.123Z | bar |
                +--------+----+----------------------+--------------------------+-----+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_inference_automatic_coercion_of_event_time_from_unixtime() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            // Note: not setting schema or adding a preprocess step to trigger inference
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson::default())
                .merge(odf::metadata::MergeStrategyAppend {})
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    let src_path = harness.temp_dir.path().join("data.ndjson");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"event_time": 1577926923, "foo": "bar"}
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                schema_inference: SchemaInferenceOpts {
                    coerce_event_time_column_type: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY foo (STRING);
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+-----+
                | offset | op | system_time          | event_time           | foo |
                +--------+----+----------------------+----------------------+-----+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T01:02:03Z | bar |
                +--------+----+----------------------+----------------------+-----+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_inference_automatic_renaming_of_conflicting_columns() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            // Note: not setting schema or adding a preprocess step to trigger inference
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson::default())
                .merge(odf::metadata::MergeStrategyAppend {})
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    let src_path = harness.temp_dir.path().join("data.ndjson");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"op": 123, "foo": "bar"}
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts {
                schema_inference: SchemaInferenceOpts {
                    coerce_event_time_column_type: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY foo (STRING);
                  OPTIONAL INT64 _op;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+-----+-----+
                | offset | op | system_time          | event_time           | foo | _op |
                +--------+----+----------------------+----------------------+-----+-----+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | bar | 123 |
                +--------+----+----------------------+----------------------+-----+-----+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// See: https://github.com/apache/datafusion/issues/7460
#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_sql_case_sensitivity() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson::default())
                .merge(odf::metadata::MergeStrategyAppend {})
                .preprocess(odf::metadata::TransformSql {
                    engine: "datafusion".into(),
                    version: None,
                    query: Some(
                        indoc!(
                            r#"
                            select
                                lower,
                                MIXed,
                                mixED,
                                mixED as mixed,
                                UPPER
                            from input
                            "#
                        )
                        .into(),
                    ),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    let src_path = harness.temp_dir.path().join("data.ndjson");
    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"lower": "lower", "MIXed": "MIXed", "mixED": "mixED", "UPPER": "UPPER"}
            "#
        ),
    )
    .unwrap();

    harness
        .ingest_from(
            target,
            None,
            DataSource::Url(url::Url::from_file_path(&src_path).unwrap()),
            PushIngestOpts::default(),
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY lower (STRING);
                  OPTIONAL BYTE_ARRAY MIXed (STRING);
                  OPTIONAL BYTE_ARRAY mixED (STRING);
                  OPTIONAL BYTE_ARRAY mixed (STRING);
                  OPTIONAL BYTE_ARRAY UPPER (STRING);
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+-------+-------+-------+-------+-------+
                | offset | op | system_time          | event_time           | lower | MIXed | mixED | mixed | UPPER |
                +--------+----+----------------------+----------------------+-------+-------+-------+-------+-------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | lower | MIXed | mixED | mixED | UPPER |
                +--------+----+----------------------+----------------------+-------+-------+-------+-------+-------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_push_with_predefined_data_schema() {
    use odf::metadata::*;

    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(ReadStepNdJson {
                    ..Default::default()
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .push_event(SetDataSchema::new(DataSchema {
            fields: vec![
                DataField::i64("offset"),
                DataField::i32("op"),
                DataField::timestamp_millis_utc("system_time"),
                DataField::timestamp_millis_utc("date"),
                DataField::string("city"),
                DataField::i64("population"),
            ],
            extra: None,
        }))
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1 - Conforming data
    let data = std::io::Cursor::new(indoc!(
        r#"
        { "date": "2020-01-01", "city": "A", "population": 1000 }
        { "date": "2020-01-02", "city": "B", "population": 2000 }
        { "date": "2020-01-03", "city": "C", "population": 3000 }
        "#
    ));

    harness
        .ingest_from(
            target.clone(),
            None,
            DataSource::Stream(Box::new(data)),
            PushIngestOpts::default(),
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | date                 | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    // Round 2 - Null in required column
    //
    // TODO: Currently we add one null and one non-null row to make read inference
    // produce and optional int column. With one null value the inferred type would
    // be Null. We should improve coercion step of the reader to allow coercing Null
    // column into any optional column type.
    let data = std::io::Cursor::new(indoc!(
        r#"
        { "date": "2020-01-04", "city": "D", "population": null }
        { "date": "2020-01-04", "city": "E", "population": 5000 }
        "#
    ));

    let res = harness
        .ingest_from(
            target,
            None,
            DataSource::Stream(Box::new(data)),
            PushIngestOpts::default(),
        )
        .await;

    assert_matches!(
        res,
        Err(PushIngestError::ExecutionError(err))
        if err.to_string().contains("Column population contains 1 null values while none were expected")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    ctx: SessionContext,
}

impl IngestTestHarness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DidGeneratorDefault>()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CacheDir::new(cache_dir))
            .add_value(CurrentAccountSubject::new_test())
            .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<InMemoryDatasetStatisticsRepository>()
            .add::<DatasetStatisticsServiceImpl>()
            .add::<QuotaCheckerStorageImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DataFormatRegistryImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .build();

        Self {
            temp_dir,
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            push_ingest_planner: catalog.get_one().unwrap(),
            push_ingest_executor: catalog.get_one().unwrap(),
            time_source: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn create_dataset(
        &self,
        dataset_snapshot: odf::DatasetSnapshot,
    ) -> odf::dataset::StoreDatasetResult {
        create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            dataset_snapshot,
            self.did_generator.generate_dataset_id().0,
            self.time_source.now(),
        )
        .await
        .unwrap()
    }

    async fn dataset_data_helper(&self, dataset_alias: &odf::DatasetAlias) -> DatasetDataHelper {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context((*resolved_dataset).clone(), self.ctx.clone())
    }

    async fn ingest_from(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        data: DataSource,
        opts: PushIngestOpts,
    ) -> Result<PushIngestResult, PushIngestError> {
        let ingest_plan = self
            .push_ingest_planner
            .plan_ingest(target.clone(), source_name, opts)
            .await
            .unwrap();

        let ingest_result = self
            .push_ingest_executor
            .execute_ingest(target.clone(), ingest_plan, data, None)
            .await?;

        if let PushIngestResult::Updated {
            old_head, new_head, ..
        } = &ingest_result
        {
            target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .unwrap();
        }

        Ok(ingest_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
