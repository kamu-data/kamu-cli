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
            // TODO: This will be replaced with `AddPushSource` event in future
            MetadataFactory::set_polling_source()
                .fetch(FetchStepUrl {
                    url: "http://localhost".to_string(),
                    event_time: Some(EventTimeSource::FromSystemTime),
                    cache: None,
                    headers: None,
                })
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
                // TODO: This will be default engine in future
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();
    let dataset_ref = DatasetAlias::new(None, dataset_name.clone()).as_local_ref();

    harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_name).await;

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
                  OPTIONAL INT64 offset;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----------------------+----------------------+------+------------+
                | offset | system_time          | date                 | city | population |
                +--------+----------------------+----------------------+------+------------+
                | 0      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                | 1      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                | 2      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .output_watermark
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
        .ingest_from_file_stream(&dataset_ref, Box::new(data), None, None)
        .await
        .unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | date                 | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 3      | 2050-01-01T12:00:00Z | 2021-01-01T00:00:00Z | C    | 4000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .output_watermark
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
            // TODO: This will be replaced with `AddPushSource` event in future
            MetadataFactory::set_polling_source()
                .fetch(FetchStepUrl {
                    url: "http://localhost".to_string(),
                    event_time: Some(EventTimeSource::FromSystemTime),
                    cache: None,
                    headers: None,
                })
                .read(ReadStepNdJson {
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..Default::default()
                })
                // TODO: This will be default engine in future
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();
    let dataset_ref = DatasetAlias::new(None, dataset_name.clone()).as_local_ref();

    harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_name).await;

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
                  OPTIONAL INT64 offset;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----------------------+----------------------+------+------------+
                | offset | system_time          | date                 | city | population |
                +--------+----------------------+----------------------+------+------------+
                | 0      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                +--------+----------------------+----------------------+------+------------+
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
                  OPTIONAL INT64 offset;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----------------------+----------------------+------+------------+
                | offset | system_time          | date                 | city | population |
                +--------+----------------------+----------------------+------+------------+
                | 1      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                +--------+----------------------+----------------------+------+------------+
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
                  OPTIONAL INT64 offset;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----------------------+----------------------+------+------------+
                | offset | system_time          | date                 | city | population |
                +--------+----------------------+----------------------+------+------------+
                | 2      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;
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
        std::fs::create_dir(&cache_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
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
            .create_dataset_from_snapshot(None, dataset_snapshot)
            .await
            .unwrap();
    }

    async fn dataset_data_helper(&self, dataset_name: &DatasetName) -> DatasetDataHelper {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_name.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context(dataset, self.ctx.clone())
    }
}
