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

use container_runtime::ContainerRuntime;
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, GzipLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::record::RowAccessor;
use futures::StreamExt;
use indoc::indoc;
use itertools::Itertools;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use tempfile::TempDir;

#[test_group::group(containerized, engine)]
#[test_log::test(tokio::test)]
async fn test_ingest_csv_with_engine() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .build(),
        )
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness
        .ingest_snapshot(dataset_snapshot, &dataset_name)
        .await;

    let parquet_reader = harness.read_datafile(&dataset_name).await;

    assert_eq!(
        parquet_reader.get_column_names(),
        ["offset", "system_time", "event_time", "city", "population"]
    );

    assert_eq!(
        parquet_reader
            .get_row_iter()
            .map(IngestTestHarness::row_mapper)
            .sorted()
            .collect::<Vec<_>>(),
        [
            (0, "A".to_owned(), 1000),
            (1, "B".to_owned(), 2000),
            (2, "C".to_owned(), 3000)
        ]
    );
}

#[test_group::group(containerized, engine)]
#[test_log::test(tokio::test)]
async fn test_ingest_parquet_with_engine() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.parquet");

    // Write data
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int32, false),
    ]));
    let cities: Arc<dyn Array> = Arc::new(StringArray::from(vec!["D", "E", "F"]));
    let populations: Arc<dyn Array> = Arc::new(Int32Array::from(vec![4000, 5000, 6000]));
    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&cities), Arc::clone(&populations)],
    )
    .unwrap();

    let arrow_writer_props = WriterProperties::builder()
        .set_compression(Compression::GZIP(GzipLevel::default()))
        .build();

    let mut arrow_writer = ArrowWriter::try_new(
        std::fs::File::create(&src_path).unwrap(),
        record_batch.schema(),
        Some(arrow_writer_props),
    )
    .unwrap();

    arrow_writer.write(&record_batch).unwrap();
    arrow_writer.close().unwrap();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStep::Parquet(ReadStepParquet {
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                }))
                .build(),
        )
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness
        .ingest_snapshot(dataset_snapshot, &dataset_name)
        .await;

    let parquet_reader = harness.read_datafile(&dataset_name).await;

    assert_eq!(
        parquet_reader.get_column_names(),
        ["offset", "system_time", "event_time", "city", "population"]
    );

    assert_eq!(
        parquet_reader
            .get_row_iter()
            .map(IngestTestHarness::row_mapper)
            .sorted()
            .collect::<Vec<_>>(),
        [
            (0, "D".to_owned(), 4000),
            (1, "E".to_owned(), 5000),
            (2, "F".to_owned(), 6000)
        ]
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    _workspace_layout: Arc<WorkspaceLayout>,
    local_repo: Arc<DatasetRepositoryLocalFs>,
    ingest_svc: Arc<IngestServiceImpl>,
}

impl IngestTestHarness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let workspace_layout = Arc::new(WorkspaceLayout::create(temp_dir.path()).unwrap());
        let local_repo = Arc::new(DatasetRepositoryLocalFs::new(
            workspace_layout.datasets_dir.clone(),
        ));

        let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
            EngineProvisionerLocalConfig::default(),
            workspace_layout.clone(),
            ContainerRuntime::default(),
        ));

        let ingest_svc = Arc::new(IngestServiceImpl::new(
            local_repo.clone(),
            engine_provisioner,
            Arc::new(ContainerRuntime::default()),
            workspace_layout.run_info_dir.clone(),
            workspace_layout.cache_dir.clone(),
        ));

        Self {
            temp_dir,
            _workspace_layout: workspace_layout,
            local_repo,
            ingest_svc,
        }
    }

    async fn ingest_snapshot(&self, dataset_snapshot: DatasetSnapshot, dataset_name: &DatasetName) {
        self.local_repo
            .create_dataset_from_snapshot(dataset_snapshot)
            .await
            .unwrap();

        let res = self
            .ingest_svc
            .ingest(
                &DatasetAlias::new(None, dataset_name.clone()).as_local_ref(),
                IngestOptions::default(),
                None,
            )
            .await;
        assert_matches!(res, Ok(IngestResult::Updated { .. }));
    }

    async fn read_datafile(&self, dataset_name: &DatasetName) -> ParquetReaderHelper {
        let dataset_ref = dataset_name.as_local_ref();
        let dataset = self.local_repo.get_dataset(&dataset_ref).await.unwrap();

        let (_, block) = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .next()
            .await
            .unwrap()
            .unwrap();

        let part_file = kamu_data_utils::data::local_url::into_local_path(
            dataset
                .as_data_repo()
                .get_internal_url(&block.event.output_data.unwrap().physical_hash)
                .await,
        )
        .unwrap();

        ParquetReaderHelper::open(&part_file)
    }

    fn row_mapper(r: datafusion::parquet::record::Row) -> (i64, String, i32) {
        (
            r.get_long(0).unwrap().clone(),
            r.get_string(3).unwrap().clone(),
            r.get_int(4).unwrap(),
        )
    }
}
