// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::prelude::*;
use dill::Component;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_push_url_stream() {
    let harness = IngestTestHarness::new();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
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
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let dataset_ref = dataset_alias.as_local_ref();

    harness.create_dataset(dataset_snapshot).await;
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
        .push_ingest_svc
        .ingest_from_url(
            &dataset_ref,
            None,
            url::Url::from_file_path(&src_path).unwrap(),
            PushIngestOpts::default(),
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
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
        .push_ingest_svc
        .ingest_from_file_stream(
            &dataset_ref,
            None,
            Box::new(data),
            PushIngestOpts::default(),
            None,
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
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(ReadStepNdJson {
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
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
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let dataset_ref = dataset_alias.as_local_ref();

    harness.create_dataset(dataset_snapshot).await;
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
        .push_ingest_svc
        .ingest_from_url(
            &dataset_ref,
            None,
            url::Url::from_file_path(&src_path).unwrap(),
            PushIngestOpts {
                media_type: Some(MediaType::CSV.to_owned()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
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
        .push_ingest_svc
        .ingest_from_url(
            &dataset_ref,
            None,
            url::Url::from_file_path(&src_path).unwrap(),
            PushIngestOpts {
                media_type: Some(MediaType::NDJSON.to_owned()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
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
        .push_ingest_svc
        .ingest_from_url(
            &dataset_ref,
            None,
            url::Url::from_file_path(&src_path).unwrap(),
            PushIngestOpts {
                media_type: Some(MediaType::JSON.to_owned()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
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
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::add_push_source()
                .read(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["event_time TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                })
                .merge(MergeStrategyAppend {})
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let dataset_ref = dataset_alias.as_local_ref();

    harness.create_dataset(dataset_snapshot).await;
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
        .push_ingest_svc
        .ingest_from_url(
            &dataset_ref,
            None,
            url::Url::from_file_path(&src_path).unwrap(),
            PushIngestOpts::default(),
            None,
        )
        .await
        .unwrap();

    let set_data_schema = data_helper.get_last_set_data_schema_block().await.event;

    // This schema is written automatically by the writer
    let schema_current = set_data_schema.schema_as_arrow().unwrap();

    // This schema is captured earlier with:
    // - kamu-cli = 'branch/breaking-changes'
    // - datafusion = 33
    //
    // To refresh use:
    // println!("{}", hex::encode(set_data_schema.schema.as_slice()));

    let schema_prev = SetDataSchema {
        schema: hex::decode(
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
            0006f66667365740000").unwrap(),
    }.schema_as_arrow().unwrap();

    kamu_ingest_datafusion::DataWriterDataFusion::validate_output_schema_equivalence(
        &schema_current,
        &schema_prev,
    )
    .expect(
        "Schema drift detected! Schema produced by the current kamu/datafusion version is not \
         equivalent to the schema produced previously. This will result in writer errors on \
         existing datasets. You'll need to investigate how exactly the schema representation \
         changed and whether equivalence test needs to be relaxed.",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    push_ingest_svc: Arc<dyn PushIngestService>,
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
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CacheDir::new(cache_dir))
            .add_value(CurrentAccountSubject::new_test())
            .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DataFormatRegistryImpl>()
            .add::<PushIngestServiceImpl>()
            .build();

        Self {
            temp_dir,
            dataset_repo: catalog.get_one().unwrap(),
            push_ingest_svc: catalog.get_one().unwrap(),
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn create_dataset(&self, dataset_snapshot: DatasetSnapshot) {
        self.dataset_repo
            .create_dataset_from_snapshot(dataset_snapshot)
            .await
            .unwrap();
    }

    async fn dataset_data_helper(&self, dataset_alias: &DatasetAlias) -> DatasetDataHelper {
        let dataset = self
            .dataset_repo
            .find_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context(dataset, self.ctx.clone())
    }
}
