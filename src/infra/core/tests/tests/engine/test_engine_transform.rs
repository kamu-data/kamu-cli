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
use futures::StreamExt;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

use crate::TransformTestHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetHelper {
    dataset: Arc<dyn odf::Dataset>,
    tempdir: PathBuf,
}

impl DatasetHelper {
    fn new(dataset: Arc<dyn odf::Dataset>, tempdir: impl Into<PathBuf>) -> Self {
        Self {
            dataset,
            tempdir: tempdir.into(),
        }
    }

    async fn block_count(&self) -> usize {
        use odf::dataset::MetadataChainExt;
        self.dataset.as_metadata_chain().iter_blocks().count().await
    }

    async fn data_slice_path(&self, data_slice: &odf::DataSlice) -> PathBuf {
        odf::utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&data_slice.physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn checkpoint_path(&self, checkpoint: &odf::Checkpoint) -> PathBuf {
        odf::utils::data::local_url::into_local_path(
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
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap();

        use odf::metadata::AsTypedBlock;
        let orig_block = self
            .dataset
            .as_metadata_chain()
            .get_block(&old_head)
            .await
            .unwrap()
            .into_typed::<odf::metadata::ExecuteTransform>()
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
                .insert_bytes(&checkpoint_data, odf::storage::InsertOpts::default())
                .await
                .unwrap()
                .hash
        };

        // Compute new hashes
        let new_slice = odf::DataSlice {
            logical_hash: odf::utils::data::hash::get_parquet_logical_hash(&tmp_path).unwrap(),
            physical_hash: odf::utils::data::hash::get_file_physical_hash(&tmp_path).unwrap(),
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

        // Overwrite last block
        self.dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                orig_block.prev_block_hash.as_ref().unwrap(),
                odf::dataset::SetRefOpts::default(),
            )
            .await
            .unwrap();

        let new_head = self
            .dataset
            .commit_event(
                odf::metadata::ExecuteTransform {
                    new_data: Some(new_slice.clone()),
                    new_checkpoint: Some(odf::Checkpoint {
                        physical_hash: new_checkpoint_hash,
                        size: 16,
                    }),
                    ..orig_block.event
                }
                .into(),
                odf::dataset::CommitOpts {
                    system_time: Some(orig_block.system_time),
                    ..odf::dataset::CommitOpts::default()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestHarness {
    tempdir: tempfile::TempDir,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    polling_ingest_svc: Arc<dyn PollingIngestService>,
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    transform_helper: TransformTestHelper,
    did_generator: Arc<dyn DidGenerator>,
    time_source: Arc<SystemTimeSourceStub>,
}

impl TestHarness {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let run_info_dir = tempdir.path().join("run");
        let cache_dir = tempdir.path().join("cache");
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DidGeneratorDefault>()
            .add_value(ContainerRuntimeConfig::default())
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CacheDir::new(cache_dir))
            .add::<ContainerRuntime>()
            .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(EngineProvisionerLocalConfig::default())
            .add::<EngineProvisionerLocal>()
            .add_value(ObjectStoreRegistryImpl::new(vec![Arc::new(
                ObjectStoreBuilderLocalFs::new(),
            )]))
            .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<FetchService>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PollingIngestServiceImpl>()
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformElaborationServiceImpl>()
            .add::<TransformExecutorImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionPlannerImpl>()
            .add::<CompactionExecutorImpl>()
            .add::<DatasetKeyValueServiceSysEnv>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .build();

        let transform_helper = TransformTestHelper::from_catalog(&catalog);

        Self {
            tempdir,
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            polling_ingest_svc: catalog.get_one().unwrap(),
            push_ingest_planner: catalog.get_one().unwrap(),
            push_ingest_executor: catalog.get_one().unwrap(),
            time_source: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            transform_helper,
        }
    }

    async fn build_metadata_state(&self, target: ResolvedDataset) -> DataWriterMetadataState {
        DataWriterMetadataState::build(target, &odf::BlockRef::Head, None)
            .await
            .unwrap()
    }

    async fn polling_ingest(
        &self,
        target: ResolvedDataset,
        metadata_state: Box<DataWriterMetadataState>,
    ) {
        let ingest_result = self
            .polling_ingest_svc
            .ingest(
                target.clone(),
                metadata_state,
                PollingIngestOptions::default(),
                None,
            )
            .await
            .unwrap();

        if let PollingIngestResult::Updated {
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
    }

    async fn push_ingest(
        &self,
        target: ResolvedDataset,
        ingest_plan: PushIngestPlan,
        data: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> PushIngestResult {
        let ingest_result = self
            .push_ingest_executor
            .execute_ingest(target.clone(), ingest_plan, DataSource::Stream(data), None)
            .await
            .unwrap();

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

        ingest_result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Remove `test_retractions` flag once RisingWave can handle them without
// crashing
async fn test_transform_common(transform: odf::metadata::Transform, test_retractions: bool) {
    let harness = TestHarness::new();

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = harness.tempdir.path().join("data.csv");
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            // TODO: Simplify using push sources
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population INT".to_string(),
                    ]),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let root_alias = root_snapshot.name.clone();

    let root_stored = create_test_dataset_from_snapshot(
        harness.dataset_registry.as_ref(),
        harness.dataset_storage_unit_writer.as_ref(),
        root_snapshot,
        harness.did_generator.generate_dataset_id().0,
        harness.time_source.now(),
    )
    .await
    .unwrap();

    let root_target = ResolvedDataset::from_stored(&root_stored, &root_alias);

    let root_metadata_state = harness.build_metadata_state(root_target.clone()).await;

    harness
        .polling_ingest(root_target.clone(), Box::new(root_metadata_state))
        .await;

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs([&root_alias.dataset_name])
                .transform(transform.clone())
                .build(),
        )
        .build();

    let deriv_alias = deriv_snapshot.name.clone();

    let deriv_stored = create_test_dataset_from_snapshot(
        harness.dataset_registry.as_ref(),
        harness.dataset_storage_unit_writer.as_ref(),
        deriv_snapshot,
        harness.did_generator.generate_dataset_id().0,
        harness.time_source.now(),
    )
    .await
    .unwrap();

    let deriv_target = ResolvedDataset::from_stored(&deriv_stored, &deriv_alias);

    let deriv_helper = DatasetHelper::new(deriv_stored.dataset.clone(), harness.tempdir.path());
    let deriv_data_helper = DatasetDataHelper::new(deriv_stored.dataset.clone());

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap());

    let res = harness
        .transform_helper
        .transform_dataset(deriv_target.clone())
        .await;
    assert_matches!(res, TransformResult::Updated { .. });

    // First transform writes two blocks: SetDataSchema, ExecuteTransform
    assert_eq!(deriv_helper.block_count().await, 4);

    let df = deriv_data_helper.get_last_data().await;
    odf::utils::testing::assert_data_eq(
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
    odf::utils::testing::assert_schema_eq(
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

    if !test_retractions {
        return;
    }

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

    let root_metadata_state = harness.build_metadata_state(root_target.clone()).await;

    harness
        .polling_ingest(root_target.clone(), Box::new(root_metadata_state))
        .await;

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 1, 3, 12, 0, 0).unwrap());

    let res = harness
        .transform_helper
        .transform_dataset(deriv_target.clone())
        .await;
    assert_matches!(res, TransformResult::Updated { .. });

    // Only one block written this time
    assert_eq!(deriv_helper.block_count().await, 5);

    let df = deriv_data_helper.get_last_data().await;
    odf::utils::testing::assert_data_eq(
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
    odf::utils::testing::assert_schema_eq(
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

    let verify_result = harness
        .transform_helper
        .verify_transform(deriv_target.clone())
        .await;

    assert_matches!(verify_result, Ok(()));

    ///////////////////////////////////////////////////////////////////////////
    // Verify - mismatching data with different logical hash
    ///////////////////////////////////////////////////////////////////////////

    deriv_helper
        .rewrite_last_data_block_with_different_data()
        .await;

    let verify_result = harness
        .transform_helper
        .verify_transform(deriv_target)
        .await;

    assert_matches!(
        verify_result,
        Err(VerifyTransformError::Execute(
            VerifyTransformExecuteError::DataNotReproducible(_)
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        true,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        true,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        true,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// See: https://github.com/kamu-data/kamu-cli/issues/599
#[test_group::group(containerized, engine, transform, risingwave)]
#[ignore = "#599 Disabled for disk space issues reason"]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_risingwave() {
    test_transform_common(
        MetadataFactory::transform()
            .engine("risingwave")
            .query(
                "SELECT
                    event_time,
                    city,
                    cast(population * 10 as int) as population_x10
                FROM root",
            )
            .build(),
        false, // TODO: RW should does not support retractions at all
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Accounts for engine-specific quirks in the schema
fn normalize_schema(s: &DFSchema, engine: &str) -> DFSchema {
    let mut fields = Vec::new();
    for (i, f) in s.fields().iter().enumerate() {
        let f = f.clone();
        let (q, _) = s.qualified_field(i);
        let f = match engine {
            // Datafusion has poor control over nullability
            "datafusion" => match f.name().as_str() {
                "offset" | "event_time" => Arc::new(f.as_ref().clone().with_nullable(false)),
                _ => f,
            },
            // Spark:
            // - produces optional `offset` and `event_time`
            "spark" => match f.name().as_str() {
                "op" => {
                    assert!(f.is_nullable());
                    Arc::new(f.as_ref().clone().with_nullable(false))
                }
                "event_time" => Arc::new(f.as_ref().clone().with_nullable(false)),
                _ => f,
            },
            // Flink:
            // - produces optional `event_time`
            "flink" => match f.name().as_str() {
                "event_time" => Arc::new(f.as_ref().clone().with_nullable(false)),
                _ => f,
            },
            // RisingWave has no control over nullability
            "risingwave" => match f.name().as_str() {
                "offset" | "event_time" => Arc::new(f.as_ref().clone().with_nullable(false)),
                _ => f,
            },
            _ => unreachable!(),
        };

        fields.push((q.cloned(), f));
    }

    DFSchema::new_with_metadata(fields, s.metadata().clone()).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_transform_empty_inputs() {
    let harness = TestHarness::new();
    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap());

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let root_stored = create_test_dataset_from_snapshot(
        harness.dataset_registry.as_ref(),
        harness.dataset_storage_unit_writer.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name("root")
            .kind(odf::DatasetKind::Root)
            .build(),
        harness.did_generator.generate_dataset_id().0,
        harness.time_source.now(),
    )
    .await
    .unwrap();

    let root_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("root"));

    let root_target = ResolvedDataset::from_stored(&root_stored, &root_alias);

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs([&odf::DatasetAlias::new(
                    None,
                    odf::DatasetName::new_unchecked("root"),
                )])
                .transform(
                    MetadataFactory::transform()
                        .engine("datafusion")
                        .query("select event_time, city, 'CA' as country, population from root")
                        .build(),
                )
                .build(),
        )
        .build();

    let deriv_alias = deriv_snapshot.name.clone();

    let deriv_stored = create_test_dataset_from_snapshot(
        harness.dataset_registry.as_ref(),
        harness.dataset_storage_unit_writer.as_ref(),
        deriv_snapshot,
        harness.did_generator.generate_dataset_id().0,
        harness.time_source.now(),
    )
    .await
    .unwrap();

    let deriv_target = ResolvedDataset::from_stored(&deriv_stored, &deriv_alias);

    // let deriv_helper = DatasetHelper::new(dataset.clone(),
    // harness.tempdir.path()); let deriv_data_helper =
    // DatasetDataHelper::new(dataset);

    ///////////////////////////////////////////////////////////////////////////
    // 1: Input doesn't have schema yet - skip update completely
    ///////////////////////////////////////////////////////////////////////////

    let res = harness
        .transform_helper
        .transform_dataset(deriv_target.clone())
        .await;
    assert_matches!(res, TransformResult::UpToDate);

    ///////////////////////////////////////////////////////////////////////////
    // 2: Input has schema, but no data - transorm will establish output schema
    ///////////////////////////////////////////////////////////////////////////

    root_stored
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population INT".to_string(),
                    ]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts {
                system_time: Some(harness.time_source.now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let ingest_plan = harness
        .push_ingest_planner
        .plan_ingest(root_target.clone(), None, PushIngestOpts::default())
        .await
        .unwrap();

    let ingest_result = harness
        .push_ingest(
            root_target.clone(),
            ingest_plan,
            Box::new(tokio::io::BufReader::new(std::io::Cursor::new(b""))),
        )
        .await;
    assert_matches!(ingest_result, PushIngestResult::Updated { .. });

    let res = harness
        .transform_helper
        .transform_dataset(deriv_target.clone())
        .await;
    assert_matches!(res, TransformResult::Updated { .. });

    let deriv_helper = DatasetDataHelper::new(deriv_stored.dataset.clone());

    deriv_helper
        .assert_latest_set_schema_eq(indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              REQUIRED BYTE_ARRAY country (STRING);
              OPTIONAL INT32 population;
            }
            "#
        ))
        .await;

    assert_eq!(deriv_helper.data_slice_count().await, 0);

    ///////////////////////////////////////////////////////////////////////////
    // 3: Input gets some data
    ///////////////////////////////////////////////////////////////////////////

    let ingest_plan = harness
        .push_ingest_planner
        .plan_ingest(root_target.clone(), None, PushIngestOpts::default())
        .await
        .unwrap();

    let ingest_result = harness
        .push_ingest(
            root_target.clone(),
            ingest_plan,
            Box::new(tokio::io::BufReader::new(std::io::Cursor::new(
                br#"{"city": "A", "population": 100}"#,
            ))),
        )
        .await;
    assert_matches!(ingest_result, PushIngestResult::Updated { .. });

    let res = harness
        .transform_helper
        .transform_dataset(deriv_target)
        .await;
    assert_matches!(res, TransformResult::Updated { .. });

    deriv_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  REQUIRED BYTE_ARRAY country (STRING);
                  OPTIONAL INT32 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+---------+------------+
                | offset | op | system_time          | event_time           | city | country | population |
                +--------+----+----------------------+----------------------+------+---------+------------+
                | 0      | 0  | 2050-01-02T12:00:00Z | 2050-01-02T12:00:00Z | A    | CA      | 100        |
                +--------+----+----------------------+----------------------+------+---------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
