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

use container_runtime::ContainerRuntime;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::record::RowAccessor;
use futures::StreamExt;
use indoc::indoc;
use itertools::Itertools;
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

    async fn get_data_of_block(&self, block_hash: &Multihash) -> ParquetReaderHelper {
        let block = self
            .dataset
            .as_metadata_chain()
            .get_block(block_hash)
            .await
            .unwrap();

        let data_path = self
            .data_slice_path(
                block
                    .as_data_stream_block()
                    .unwrap()
                    .event
                    .output_data
                    .unwrap(),
            )
            .await;

        ParquetReaderHelper::open(&data_path)
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
            .into_typed::<ExecuteQuery>()
            .unwrap();

        let orig_slice = orig_block.event.output_data.as_ref().unwrap();
        let orig_data_path = self.data_slice_path(&orig_slice).await;

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
            interval: orig_slice.interval.clone(),
            size: std::fs::metadata(&tmp_path).unwrap().len() as i64,
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
        if let Some(orig_checkpoint) = &orig_block.event.output_checkpoint {
            std::fs::remove_file(self.checkpoint_path(&orig_checkpoint).await).unwrap();
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
                ExecuteQuery {
                    output_data: Some(new_slice.clone()),
                    output_checkpoint: Some(Checkpoint {
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

        tracing::warn!(%old_head, %new_head, ?orig_slice, ?new_slice, "Re-written last ExecuteQuery block");
    }

    async fn rewrite_last_data_block_with_equivalent_different_encoding(&self) {
        self.rewrite_last_data_block_with_different_encoding(None)
            .await
    }

    async fn rewrite_last_data_block_with_different_data(&self) {
        self.rewrite_last_data_block_with_different_encoding(Some(Box::new(|b| {
            b.slice(1, b.num_rows() - 1)
        })))
        .await
    }
}

async fn test_transform_common(transform: Transform) {
    let tempdir = tempfile::tempdir().unwrap();
    let run_info_dir = tempdir.path().join("run");
    let cache_dir = tempdir.path().join("cache");
    std::fs::create_dir(&run_info_dir).unwrap();
    std::fs::create_dir(&cache_dir).unwrap();

    let dataset_repo = Arc::new(
        DatasetRepositoryLocalFs::create(
            tempdir.path().join("datasets"),
            Arc::new(CurrentAccountSubject::new_test()),
            Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
            false,
        )
        .unwrap(),
    );
    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        ContainerRuntime::default(),
        dataset_repo.clone(),
        run_info_dir.clone(),
    ));

    let dataset_action_authorizer = Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new());

    let ingest_svc = IngestServiceImpl::new(
        dataset_repo.clone(),
        dataset_action_authorizer.clone(),
        engine_provisioner.clone(),
        Arc::new(ObjectStoreRegistryImpl::new(vec![Arc::new(
            ObjectStoreBuilderLocalFs::new(),
        )])),
        Arc::new(ContainerRuntime::default()),
        run_info_dir,
        cache_dir,
        Arc::new(SystemTimeSourceDefault),
    );

    let transform_svc = TransformServiceImpl::new(
        dataset_repo.clone(),
        dataset_action_authorizer.clone(),
        engine_provisioner.clone(),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = tempdir.path().join("data.csv");
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

    let root_snapshot = MetadataFactory::dataset_snapshot()
        .name("root")
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
                // TODO: Temporary no-op to make ingest use experimental DataFusion engine
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    query: Some("select * from input".to_string()),
                    version: None,
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .build();

    let root_alias = DatasetAlias::new(None, root_snapshot.name.clone());

    dataset_repo
        .create_dataset_from_snapshot(None, root_snapshot)
        .await
        .unwrap();

    ingest_svc
        .polling_ingest(
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
            MetadataFactory::set_transform([&root_alias.dataset_name])
                .transform(transform)
                .build(),
        )
        .build();

    let deriv_alias = DatasetAlias::new(None, deriv_snapshot.name.clone());

    let dataset = dataset_repo
        .create_dataset_from_snapshot(None, deriv_snapshot)
        .await
        .unwrap()
        .dataset;

    let deriv_helper = DatasetHelper::new(dataset, tempdir.path());

    let block_hash = match transform_svc
        .transform(&deriv_alias.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    assert_eq!(deriv_helper.block_count().await, 3);

    let parquet_reader = deriv_helper.get_data_of_block(&block_hash).await;

    assert_eq!(
        parquet_reader.get_column_names(),
        [
            "offset",
            "system_time",
            "event_time",
            "city",
            "population_x10"
        ]
    );

    assert_eq!(
        parquet_reader
            .get_row_iter()
            .map(|r| r.unwrap())
            .map(|r| {
                (
                    r.get_long(0).unwrap().clone(),
                    r.get_string(3).unwrap().clone(),
                    r.get_int(4).unwrap(),
                )
            })
            .sorted()
            .collect::<Vec<_>>(),
        [
            (0, "A".to_owned(), 10000),
            (1, "B".to_owned(), 20000),
            (2, "C".to_owned(), 30000)
        ]
    );

    ///////////////////////////////////////////////////////////////////////////
    // Round 2
    ///////////////////////////////////////////////////////////////////////////

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            D,4000
            E,5000
            "
        ),
    )
    .unwrap();

    ingest_svc
        .polling_ingest(
            &root_alias.as_local_ref(),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_alias.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let parquet_reader = deriv_helper.get_data_of_block(&block_hash).await;

    assert_eq!(
        parquet_reader
            .get_row_iter()
            .map(|r| r.unwrap())
            .map(|r| {
                (
                    r.get_long(0).unwrap().clone(),
                    r.get_string(3).unwrap().clone(),
                    r.get_int(4).unwrap(),
                )
            })
            .sorted()
            .collect::<Vec<_>>(),
        [(3, "D".to_owned(), 40000), (4, "E".to_owned(), 50000),]
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

    assert_matches!(verify_result, Ok(VerificationResult::Valid));

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
            .query("SELECT event_time, city, population * 10 as population_x10 FROM root")
            .build(),
    )
    .await
}

#[test_group::group(containerized, engine, transform, flink)]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_flink() {
    test_transform_common(
        MetadataFactory::transform()
            .engine("flink")
            .query("SELECT event_time, city, population * 10 as population_x10 FROM root")
            .build(),
    )
    .await
}

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_transform_with_engine_datafusion() {
    test_transform_common(
        MetadataFactory::transform()
            .engine("datafusion")
            .query(
                "SELECT event_time, city, cast(population * 10 as int) as population_x10 FROM root",
            )
            .build(),
    )
    .await
}
