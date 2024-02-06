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

use chrono::{TimeZone, Utc};
use container_runtime::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use dill::Component;
use event_bus::EventBus;
use futures::StreamExt;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;

struct DatasetHelper {
    dataset: Arc<dyn Dataset>,
    tempdir: PathBuf,
}

impl DatasetHelper {
    fn new(dataset: Arc<dyn Dataset>, tempdir: impl Into<PathBuf>) -> Self {
        Self {
            dataset,
            tempdir: tempdir.into(),
        }
    }

    async fn block_count(&self) -> usize {
        self.dataset.as_metadata_chain().iter_blocks().count().await
    }

    async fn data_slice_path(&self, data_slice: &DataSlice) -> PathBuf {
        kamu_data_utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&data_slice.physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn checkpoint_path(&self, checkpoint: &Checkpoint) -> PathBuf {
        kamu_data_utils::data::local_url::into_local_path(
            self.dataset
                .as_checkpoint_repo()
                .get_internal_url(&checkpoint.physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn rewrite_last_data_block_with_different_encoding(
        &self,
        mutate_data: Option<Box<dyn FnOnce(RecordBatch) -> RecordBatch>>,
    ) {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::schema::types::ColumnPath;

        // Get last block
        let old_head = self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .unwrap();

        let orig_block = self
            .dataset
            .as_metadata_chain()
            .get_block(&old_head)
            .await
            .unwrap()
            .into_typed::<ExecuteTransform>()
            .unwrap();

        let orig_slice = orig_block.event.new_data.as_ref().unwrap();
        let orig_data_path = self.data_slice_path(orig_slice).await;

        // Re-encode data
        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&orig_data_path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        let (mutated, record_batch) = if let Some(mutate_data) = mutate_data {
            (true, mutate_data(record_batch))
        } else {
            (false, record_batch)
        };

        let tmp_path: std::path::PathBuf = self.tempdir.join(".tmpdata");
        let mut arrow_writer = ArrowWriter::try_new(
            std::fs::File::create(&tmp_path).unwrap(),
            record_batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(1)
                    .set_column_dictionary_enabled(
                        ColumnPath::new(vec!["city".to_string(), "population_x10".to_string()]),
                        true,
                    )
                    .build(),
            ),
        )
        .unwrap();

        arrow_writer.write(&record_batch).unwrap();
        arrow_writer.close().unwrap();

        // Write a dummy checkpoint
        let new_checkpoint_hash = {
            use rand::RngCore;
            let mut checkpoint_data = [0u8; 128];
            rand::thread_rng().fill_bytes(&mut checkpoint_data);

            self.dataset
                .as_checkpoint_repo()
                .insert_bytes(&checkpoint_data, InsertOpts::default())
                .await
                .unwrap()
                .hash
        };

        // Compute new hashes
        let new_slice = DataSlice {
            logical_hash: kamu_data_utils::data::hash::get_parquet_logical_hash(&tmp_path).unwrap(),
            physical_hash: kamu_data_utils::data::hash::get_file_physical_hash(&tmp_path).unwrap(),
            offset_interval: orig_slice.offset_interval.clone(),
            size: std::fs::metadata(&tmp_path).unwrap().len(),
        };

        assert_ne!(new_slice.size, orig_slice.size);
        assert_ne!(new_slice.physical_hash, orig_slice.physical_hash);
        if !mutated {
            assert_eq!(new_slice.logical_hash, orig_slice.logical_hash);
        }

        // Rename new file according to new physical hash and delete the original data
        // and checkpoint
        let new_data_path = self.data_slice_path(&new_slice).await;
        std::fs::rename(&tmp_path, &new_data_path).unwrap();
        std::fs::remove_file(&orig_data_path).unwrap();
        if let Some(orig_checkpoint) = &orig_block.event.new_checkpoint {
            std::fs::remove_file(self.checkpoint_path(orig_checkpoint).await).unwrap();
        }

        // Rewrite last block
        self.dataset
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                orig_block.prev_block_hash.as_ref().unwrap(),
                SetRefOpts::default(),
            )
            .await
            .unwrap();

        let new_head = self
            .dataset
            .commit_event(
                ExecuteTransform {
                    new_data: Some(new_slice.clone()),
                    new_checkpoint: Some(Checkpoint {
                        physical_hash: new_checkpoint_hash,
                        size: 16,
                    }),
                    ..orig_block.event
                }
                .into(),
                CommitOpts {
                    system_time: Some(orig_block.system_time),
                    ..CommitOpts::default()
                },
            )
            .await
            .unwrap()
            .new_head;

        tracing::warn!(%old_head, %new_head, ?orig_slice, ?new_slice, "Re-written last ExecuteTransform block");
    }

    async fn rewrite_last_data_block_with_equivalent_different_encoding(&self) {
        self.rewrite_last_data_block_with_different_encoding(None)
            .await;
    }

    async fn rewrite_last_data_block_with_different_data(&self) {
        self.rewrite_last_data_block_with_different_encoding(Some(Box::new(|b| {
            b.slice(1, b.num_rows() - 1)
        })))
        .await;
    }
}

async fn test_transform_common(transform: Transform) {
    let tempdir = tempfile::tempdir().unwrap();
    let run_info_dir = tempdir.path().join("run");
    let cache_dir = tempdir.path().join("cache");
    std::fs::create_dir(&run_info_dir).unwrap();
    std::fs::create_dir(&cache_dir).unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add_value(ContainerRuntimeConfig::default())
        .add::<ContainerRuntime>()
        .add::<EventBus>()
        .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<kamu::DependencyGraphServiceInMemory>()
        .add_value(CurrentAccountSubject::new_test())
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add_builder(
            EngineProvisionerLocal::builder()
                .with_config(EngineProvisionerLocalConfig::default())
                .with_run_info_dir(run_info_dir.clone()),
        )
        .bind::<dyn EngineProvisioner, EngineProvisionerLocal>()
        .add_value(ObjectStoreRegistryImpl::new(vec![Arc::new(
            ObjectStoreBuilderLocalFs::new(),
        )]))
        .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>()
        .add::<DataFormatRegistryImpl>()
        .add_builder(
            PollingIngestServiceImpl::builder()
                .with_cache_dir(cache_dir)
                .with_run_info_dir(run_info_dir),
        )
        .bind::<dyn PollingIngestService, PollingIngestServiceImpl>()
        .add::<TransformServiceImpl>()
        .add_value(SystemTimeSourceStub::new_set(
            Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
        ))
        .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
        .build();

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let ingest_svc = catalog.get_one::<dyn PollingIngestService>().unwrap();
    let transform_svc = catalog.get_one::<dyn TransformService>().unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = tempdir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,10
            B,20
            C,30
            "
        ),
    )
    .unwrap();

    let root_snapshot = MetadataFactory::dataset_snapshot()
        .name("root")
        .kind(DatasetKind::Root)
        .push_event(
            // TODO: Simplify using push sources
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population INT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .merge(MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let root_alias = root_snapshot.name.clone();

    dataset_repo
        .create_dataset_from_snapshot(root_snapshot)
        .await
        .unwrap();

    ingest_svc
        .ingest(
            &root_alias.as_local_ref(),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs([&root_alias.dataset_name])
                .transform(transform.clone())
                .build(),
        )
        .build();

    let deriv_alias = deriv_snapshot.name.clone();

    let dataset_ref = dataset_repo
        .create_dataset_from_snapshot(deriv_snapshot)
        .await
        .unwrap()
        .dataset_handle
        .as_local_ref();

    let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();
    time_source.set(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap());

    let res = transform_svc
        .transform(&deriv_alias.as_local_ref(), None)
        .await
        .unwrap();
    assert_matches!(res, TransformResult::Updated { .. });

    // First transform writes two blocks: SetDataSchema, ExecuteTransform
    let dataset = dataset_repo.get_dataset(&dataset_ref).await.unwrap();
    let deriv_helper = DatasetHelper::new(dataset.clone(), tempdir.path());

    assert_eq!(deriv_helper.block_count().await, 4);

    let deriv_data_helper = DatasetDataHelper::new(dataset);
    let df = deriv_data_helper.get_last_data().await;
    kamu_data_utils::testing::assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+----------------+
            | offset | op | system_time          | event_time           | city | population_x10 |
            +--------+----+----------------------+----------------------+------+----------------+
            | 0      | 0  | 2050-01-02T12:00:00Z | 2050-01-01T12:00:00Z | A    | 100            |
            | 1      | 0  | 2050-01-02T12:00:00Z | 2050-01-01T12:00:00Z | B    | 200            |
            | 2      | 0  | 2050-01-02T12:00:00Z | 2050-01-01T12:00:00Z | C    | 300            |
            +--------+----+----------------------+----------------------+------+----------------+
            "#
        ),
    )
    .await;
    kamu_data_utils::testing::assert_schema_eq(
        &normalize_schema(df.schema(), transform.engine()),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT32 population_x10;
            }
            "#
        ),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Round 2
    ///////////////////////////////////////////////////////////////////////////

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,10
            C,35
            D,40
            E,50
            "
        ),
    )
    .unwrap();

    ingest_svc
        .ingest(
            &root_alias.as_local_ref(),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    time_source.set(Utc.with_ymd_and_hms(2050, 1, 3, 12, 0, 0).unwrap());

    let res = transform_svc
        .transform(&deriv_alias.as_local_ref(), None)
        .await
        .unwrap();
    assert_matches!(res, TransformResult::Updated { .. });

    // Only one block written this time
    let dataset = dataset_repo.get_dataset(&dataset_ref).await.unwrap();
    let deriv_helper = DatasetHelper::new(dataset.clone(), tempdir.path());

    assert_eq!(deriv_helper.block_count().await, 5);

    let deriv_data_helper = DatasetDataHelper::new(dataset);
    let df = deriv_data_helper.get_last_data().await;

    kamu_data_utils::testing::assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+----------------+
            | offset | op | system_time          | event_time           | city | population_x10 |
            +--------+----+----------------------+----------------------+------+----------------+
            | 3      | 1  | 2050-01-03T12:00:00Z | 2050-01-01T12:00:00Z | B    | 200            |
            | 4      | 2  | 2050-01-03T12:00:00Z | 2050-01-01T12:00:00Z | C    | 300            |
            | 5      | 3  | 2050-01-03T12:00:00Z | 2050-01-02T12:00:00Z | C    | 350            |
            | 6      | 0  | 2050-01-03T12:00:00Z | 2050-01-02T12:00:00Z | D    | 400            |
            | 7      | 0  | 2050-01-03T12:00:00Z | 2050-01-02T12:00:00Z | E    | 500            |
            +--------+----+----------------------+----------------------+------+----------------+
            "#
        ),
    )
    .await;
    kamu_data_utils::testing::assert_schema_eq(
        &normalize_schema(df.schema(), transform.engine()),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT32 population_x10;
            }
            "#
        ),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Verify - equivalent data with different encoding
    ///////////////////////////////////////////////////////////////////////////

    deriv_helper
        .rewrite_last_data_block_with_equivalent_different_encoding()
        .await;

    let verify_result = transform_svc
        .verify_transform(&deriv_alias.as_local_ref(), (None, None), None)
        .await;

    assert_matches!(verify_result, Ok(()));

    ///////////////////////////////////////////////////////////////////////////
    // Verify - mismatching data with different logical hash
    ///////////////////////////////////////////////////////////////////////////

    deriv_helper
        .rewrite_last_data_block_with_different_data()
        .await;

    let verify_result = transform_svc
        .verify_transform(&deriv_alias.as_local_ref(), (None, None), None)
        .await;

    assert_matches!(
        verify_result,
        Err(VerificationError::DataNotReproducible(_))
    );
}

#[test_group::group(containerized, engine, transform, spark)]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_spark() {
    test_transform_common(
        MetadataFactory::transform()
            .engine("spark")
            .query(
                "SELECT
                    op,
                    event_time,
                    city,
                    population * 10 as population_x10
                FROM root",
            )
            .build(),
    )
    .await;
}

#[test_group::group(containerized, engine, transform, flink)]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_flink() {
    // TODO: Remove `op` filed once Flink support input corrections/retractions
    // See: https://github.com/kamu-data/kamu-engine-flink/issues/11
    test_transform_common(
        MetadataFactory::transform()
            .engine("flink")
            .query(
                "SELECT
                    `op`,
                    `event_time`,
                    city,
                    population * 10 as population_x10
                FROM root",
            )
            .build(),
    )
    .await;
}

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_datafusion() {
    test_transform_common(
        MetadataFactory::transform()
            .engine("datafusion")
            .query(
                "SELECT
                    op,
                    event_time,
                    city,
                    cast(population * 10 as int) as population_x10
                FROM root",
            )
            .build(),
    )
    .await;
}

/// Accounts for engine-specific quirks in the schema
fn normalize_schema(s: &DFSchema, engine: &str) -> DFSchema {
    DFSchema::new_with_metadata(
        s.fields()
            .iter()
            .map(|f| {
                match engine {
                    // Datafusion has poor control over nullability
                    "datafusion" => match f.name().as_str() {
                        "offset" | "event_time" => f.clone().with_nullable(false),
                        _ => f.clone(),
                    },
                    // Spark:
                    // - produces optional `offset` and `event_time`
                    "spark" => match f.name().as_str() {
                        "op" => {
                            assert!(f.is_nullable());
                            f.clone().with_nullable(false)
                        }
                        "event_time" => f.clone().with_nullable(false),
                        _ => f.clone(),
                    },
                    // Flink:
                    // - produces optional `event_time`
                    "flink" => match f.name().as_str() {
                        "event_time" => f.clone().with_nullable(false),
                        _ => f.clone(),
                    },
                    _ => unreachable!(),
                }
            })
            .collect::<Vec<_>>(),
        s.metadata().clone(),
    )
    .unwrap()
}
