// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::prelude::*;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::DatasetRepositoryLocalFs;
use kamu_core::*;
use kamu_data_utils::testing::{assert_data_eq, assert_schema_eq};
use kamu_ingest_datafusion::*;
use odf::{AsTypedBlock, DatasetAlias};
use opendatafabric as odf;

///////////////////////////////////////////////////////////////
// TODO: This test belongs in kamu-ingest-datafusion crate.
// We currently cannot move it there as it needs DatasetRepositoryLocalFs to
// function. We should move it there once we further decompose the kamu core
// crate.
///////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_happy_path() {
    let mut harness = Harness::new(
        MetadataFactory::set_polling_source()
            .merge(odf::MergeStrategySnapshot {
                primary_key: vec!["city".to_string()],
                compare_columns: None,
                observation_column: None,
                obsv_added: None,
                obsv_changed: None,
                obsv_removed: None,
            })
            .build(),
    )
    .await;

    // Round 1
    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY observed (STRING);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+----------+------+------------+
            | offset | system_time          | event_time           | observed | city | population |
            +--------+----------------------+----------------------+----------+------+------------+
            | 0      | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | I        | A    | 1000       |
            | 1      | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | I        | B    | 2000       |
            | 2      | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | I        | C    | 3000       |
            +--------+----------------------+----------------------+----------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block.event.output_watermark.as_ref(),
        Some(&harness.source_event_time)
    );

    // Round 2
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 2, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 2, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY observed (STRING);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+----------+------+------------+
            | offset | system_time          | event_time           | observed | city | population |
            +--------+----------------------+----------------------+----------+------+------------+
            | 3      | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | I        | D    | 4000       |
            +--------+----------------------+----------------------+----------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block.event.output_watermark.as_ref(),
        Some(&harness.source_event_time)
    );

    // Round 3 (nothing to commit)
    let prev_watermark = res.new_block.event.output_watermark.unwrap();
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 3, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 3, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await;

    assert_matches!(res, Err(WriteDataError::EmptyCommit(_)));

    // Round 4 (nothing but source state changed)
    let source_state = odf::SourceState {
        kind: "odf/etag".to_string(),
        source: "odf/poll".to_string(),
        value: "123".to_string(),
    };

    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 4, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 4, 12, 0, 0).unwrap());

    let res = harness
        .write_opts(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
            Some(source_state.clone()),
        )
        .await
        .unwrap();

    assert_eq!(res.new_block.event.output_data, None);
    // Watermark is carried
    assert_eq!(res.new_block.event.output_watermark, Some(prev_watermark));
    // Source state updated
    assert_eq!(res.new_block.event.source_state, Some(source_state));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_orders_by_event_time() {
    let mut harness = Harness::new(
        MetadataFactory::set_polling_source()
            .merge(odf::MergeStrategy::Append)
            .build(),
    )
    .await;

    let res = harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2021-01-01,A,1000
                2023-01-01,B,2000
                2022-01-01,C,3000
                "#
            ),
            "event_time DATE, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT32 event_time (DATE);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+------------+------+------------+
            | offset | system_time          | event_time | city | population |
            +--------+----------------------+------------+------+------------+
            | 0      | 2010-01-01T12:00:00Z | 2021-01-01 | A    | 1000       |
            | 1      | 2010-01-01T12:00:00Z | 2022-01-01 | C    | 3000       |
            | 2      | 2010-01-01T12:00:00Z | 2023-01-01 | B    | 2000       |
            +--------+----------------------+------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block
            .event
            .output_watermark
            .as_ref()
            .map(|dt| dt.to_rfc3339()),
        Some("2023-01-01T00:00:00+00:00".to_string())
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_normalizes_timestamps_to_utc_millis() {
    let mut harness = Harness::new(
        MetadataFactory::set_polling_source()
            .merge(odf::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build(),
    )
    .await;

    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2000-01-01,A,1000
                2000-01-01,B,2000
                2000-01-01,C,3000
                "#
            ),
            "event_time TIMESTAMP, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | event_time           | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 0      | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | A    | 1000       |
            | 1      | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | B    | 2000       |
            | 2      | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | C    | 3000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_optimal_parquet_encoding() {
    use ::datafusion::parquet::basic::{Compression, Encoding, PageType};
    use ::datafusion::parquet::file::reader::FileReader;

    let mut harness = Harness::new(
        MetadataFactory::set_polling_source()
            .merge(odf::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build(),
    )
    .await;

    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2020-01-01,A,1000
                2020-01-01,B,2000
                2020-01-01,C,3000
                "#
            ),
            "event_time TIMESTAMP, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let parquet = kamu::testing::ParquetReaderHelper::open(&harness.get_last_data_file().await);
    let meta = parquet.reader.metadata();

    // TODO: Migrate to Parquet v2 and DATA_PAGE_V2
    let assert_data_encoding = |col, enc| {
        let data_page = parquet
            .reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(col)
            .unwrap()
            .map(|p| p.unwrap())
            .filter(|p| p.page_type() == PageType::DATA_PAGE)
            .next()
            .unwrap();

        assert_eq!(data_page.encoding(), enc);
    };

    assert_eq!(meta.num_row_groups(), 1);

    let offset_col = meta.row_group(0).column(0);
    assert_eq!(offset_col.column_path().string(), "offset");
    assert_eq!(offset_col.compression(), Compression::SNAPPY);

    // TODO: Validate the encoding
    // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
    // assert_data_encoding(0, Encoding::DELTA_BINARY_PACKED);

    let system_time_col = meta.row_group(0).column(1);
    assert_eq!(system_time_col.column_path().string(), "system_time");
    assert_eq!(system_time_col.compression(), Compression::SNAPPY);
    assert_data_encoding(1, Encoding::RLE_DICTIONARY);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    temp_dir: tempfile::TempDir,
    dataset: Arc<dyn Dataset>,
    writer: DataWriterDataFusion,
    ctx: SessionContext,

    system_time: DateTime<Utc>,
    source_event_time: DateTime<Utc>,
}

impl Harness {
    async fn new(source: odf::SetPollingSource) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let system_time = Utc.with_ymd_and_hms(2010, 1, 1, 12, 0, 0).unwrap();

        let dataset_repo = Arc::new(
            DatasetRepositoryLocalFs::create(
                temp_dir.path().join("datasets"),
                Arc::new(CurrentAccountSubject::new_test()),
                Arc::new(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new()),
                false,
            )
            .unwrap(),
        );

        let dataset = dataset_repo
            .create_dataset(
                &DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
                .system_time(system_time.clone())
                .build_typed(),
            )
            .await
            .unwrap()
            .dataset;

        dataset
            .commit_event(
                source.into(),
                CommitOpts {
                    system_time: Some(system_time.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        let writer = DataWriterDataFusion::builder(dataset.clone(), ctx.clone())
            .build()
            .await
            .unwrap();

        Self {
            temp_dir,
            dataset,
            writer,
            ctx,
            system_time,
            source_event_time: Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap(),
        }
    }

    fn set_system_time(&mut self, t: DateTime<Utc>) {
        self.system_time = t;
    }

    fn set_source_event_time(&mut self, t: DateTime<Utc>) {
        self.source_event_time = t;
    }

    async fn write_opts(
        &mut self,
        data: &str,
        schema: &str,
        source_state: Option<odf::SourceState>,
    ) -> Result<WriteDataResult, WriteDataError> {
        let df = if data.len() == 0 {
            None
        } else {
            let data_path = self.temp_dir.path().join("data.bin");
            std::fs::write(&data_path, data).unwrap();

            let df = ReaderCsv::new(
                self.ctx.clone(),
                odf::ReadStepCsv {
                    header: Some(true),
                    schema: Some(schema.split(',').map(|s| s.to_string()).collect()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .read(&data_path)
            .await
            .unwrap();

            Some(df)
        };

        self.writer
            .write(
                df,
                WriteDataOpts {
                    system_time: self.system_time.clone(),
                    source_event_time: self.source_event_time.clone(),
                    source_state,
                    data_staging_path: self.temp_dir.path().join("write.tmp"),
                },
            )
            .await
    }

    async fn write(&mut self, data: &str, schema: &str) -> Result<WriteDataResult, WriteDataError> {
        self.write_opts(data, schema, None).await
    }

    async fn get_last_data_block(&self) -> odf::MetadataBlockTyped<odf::AddData> {
        use futures::StreamExt;

        let (_, block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .next()
            .await
            .unwrap()
            .unwrap();
        block.into_typed::<odf::AddData>().unwrap()
    }

    async fn get_last_data_file(&self) -> PathBuf {
        let block = self.get_last_data_block().await;

        kamu_data_utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&block.event.output_data.unwrap().physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn get_last_data(&self) -> DataFrame {
        let part_file = self.get_last_data_file().await;
        self.ctx
            .read_parquet(
                part_file.to_string_lossy().as_ref(),
                ParquetReadOptions {
                    file_extension: part_file
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }
}
