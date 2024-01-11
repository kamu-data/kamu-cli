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
use event_bus::EventBus;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

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
                            .map(|s| s.to_string())
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
            None,
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset (INTEGER(64,false));
                  REQUIRED INT32 op (INTEGER(8,false));
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
        .ingest_from_file_stream(&dataset_ref, None, Box::new(data), None, None)
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

/////////////////////////////////////////////////////////////////////////////////////////

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
                            .map(|s| s.to_string())
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
            Some(MediaType::CSV.to_owned()),
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset (INTEGER(64,false));
                  REQUIRED INT32 op (INTEGER(8,false));
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
            Some(MediaType::NDJSON.to_owned()),
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset (INTEGER(64,false));
                  REQUIRED INT32 op (INTEGER(8,false));
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
            Some(MediaType::JSON.to_owned()),
            None,
        )
        .await
        .unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset (INTEGER(64,false));
                  REQUIRED INT32 op (INTEGER(8,false));
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

/////////////////////////////////////////////////////////////////////////////////////////

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
                            .map(|s| s.to_string())
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
            None,
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
            "0c0000000800080000000400080000000400000006000000580100001c010000c40000008\
            4000000440000000400000098ffffff1800000020000000000001021c00000008000c00040\
            00b00080000004000000000000001000000000a000000706f70756c6174696f6e0000d4fff\
            fff180000000c0000000000010510000000000000000400040004000000040000006369747\
            9000000001000140010000e000f0004000000080010000000140000000c0000000000010a1\
            c00000000000000c4ffffff080000000000010003000000555443000a0000006576656e745\
            f74696d650000bcffffff1c0000000c0000000000000a240000000000000008000c000a000\
            40008000000080000000000010003000000555443000b00000073797374656d5f74696d650\
            010001400100000000f000400000008001000000010000000140000000000000210000000c\
            6ffffff0800000000000000020000006f7000001000160010000e000f00040000000800100\
            00000180000001c00000000000102180000000000060008000400060000004000000000000\
            000060000006f66667365740000").unwrap(),
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

/////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    push_ingest_svc: Arc<dyn PushIngestService>,
    ctx: SessionContext,
}

impl IngestTestHarness {
    fn new() -> Self {
        Self::new_with_authorizer(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new())
    }

    fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        dataset_action_authorizer: TDatasetAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(cache_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(temp_dir.path().join("datasets"))
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add_builder(
                PushIngestServiceImpl::builder()
                    .with_object_store_registry(Arc::new(ObjectStoreRegistryImpl::new(vec![
                        Arc::new(ObjectStoreBuilderLocalFs::new()),
                    ])))
                    .with_data_format_registry(Arc::new(DataFormatRegistryImpl::new()))
                    .with_run_info_dir(run_info_dir),
            )
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let push_ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();

        Self {
            temp_dir,
            dataset_repo,
            push_ingest_svc,
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
            .get_dataset(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context(dataset, self.ctx.clone())
    }
}
