// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use dill::Component;
use kamu::domain::*;
use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer, ParquetWriterHelper};
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac_inmem::RebacRepositoryInMem;
use kamu_auth_rebac_services::RebacServiceImpl;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

use super::test_pull_service_impl::TestTransformService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_data_consistency() {
    let tempdir = tempfile::tempdir().unwrap();
    let datasets_dir = tempdir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let catalog = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(
            MockDatasetActionAuthorizer::new().expect_check_read_dataset(&dataset_alias, 3, true),
        )
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
        .add_value(TestTransformService::new(Arc::new(Mutex::new(Vec::new()))))
        .bind::<dyn TransformService, TestTransformService>()
        .add::<VerificationServiceImpl>()
        .add::<RebacRepositoryInMem>()
        .add::<RebacServiceImpl>()
        .build();

    let verification_svc = catalog.get_one::<dyn VerificationService>().unwrap();
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_repo_writer = catalog.get_one::<dyn DatasetRepositoryWriter>().unwrap();

    dataset_repo_writer
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await
        .unwrap();

    dataset_repo_writer
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias.clone())
                .kind(DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(["foo"])
                        .build(),
                )
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await
        .unwrap();

    assert_matches!(
        verification_svc
            .verify(
                &dataset_alias.as_local_ref(),
                (None, None),
                VerificationOptions {
                    check_integrity: true,
                    check_logical_hashes: true,
                    replay_transformations: false
                },
                None,
            )
            .await,
        VerificationResult {
            outcome: Ok(()),
            ..
        }
    );

    // Write data
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let a: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
    let data_path = tempdir.path().join("data");

    ParquetWriterHelper::from_record_batch(&data_path, &record_batch).unwrap();
    let data_logical_hash =
        kamu_data_utils::data::hash::get_parquet_logical_hash(&data_path).unwrap();
    let data_physical_hash =
        kamu_data_utils::data::hash::get_file_physical_hash(&data_path).unwrap();

    // Commit data
    let dataset = dataset_repo
        .find_dataset_by_ref(&dataset_alias.as_local_ref())
        .await
        .unwrap();

    let head = dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(OffsetInterval { start: 0, end: 0 }),
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(data_path)),
            None,
            CommitOpts::default(),
        )
        .await
        .unwrap()
        .new_head;

    assert_matches!(
        dataset.as_metadata_chain().get_block(&head).await.unwrap(),
        MetadataBlock {
            event: MetadataEvent::AddData(AddData {
                new_data: Some(DataSlice {
                    logical_hash,
                    physical_hash,
                    ..
                }),
                ..
            }),
        ..
    } if logical_hash == data_logical_hash && physical_hash == data_physical_hash);

    // Check verification succeeds
    assert_matches!(
        verification_svc
            .verify(
                &dataset_alias.as_local_ref(),
                (None, None),
                VerificationOptions {
                    check_integrity: true,
                    check_logical_hashes: true,
                    replay_transformations: false
                },
                None,
            )
            .await,
        VerificationResult {
            outcome: Ok(()),
            ..
        }
    );

    // Overwrite with different data
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "f", "e"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();

    let local_data_path = kamu_data_utils::data::local_url::into_local_path(
        dataset
            .as_data_repo()
            .get_internal_url(&data_physical_hash)
            .await,
    )
    .unwrap();

    ParquetWriterHelper::from_record_batch(&local_data_path, &record_batch).unwrap();

    // Check verification fails
    assert_matches!(
        verification_svc.verify(
            &dataset_alias.as_local_ref(),
            (None, None),
            VerificationOptions {check_integrity: true, check_logical_hashes: true, replay_transformations: false},
            None,
        ).await,
        VerificationResult {
            outcome: Err(VerificationError::DataDoesNotMatchMetadata(
                DataDoesNotMatchMetadata {
                    block_hash,
                    error: DataVerificationError::LogicalHashMismatch { expected, .. },
                }
            )),
            ..
        } if block_hash == head && expected == data_logical_hash,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
