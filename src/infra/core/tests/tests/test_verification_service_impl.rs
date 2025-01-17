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

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use kamu::domain::*;
use kamu::testing::{BaseRepoHarness, MetadataFactory, ParquetWriterHelper};
use kamu::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_data_consistency() {
    let harness = VerifyHarness::new();

    let foo_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let bar_alias = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let foo = harness.create_root_dataset(&foo_alias).await;
    foo.dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let bar = harness
        .create_derived_dataset(&bar_alias, vec![foo_alias.as_local_ref()])
        .await;
    bar.dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    assert_matches!(
        harness
            .verification_svc
            .verify(
                VerificationRequest {
                    target: ResolvedDataset::from(&bar),
                    block_range: (None, None),
                    options: VerificationOptions {
                        check_integrity: true,
                        check_logical_hashes: true,
                        replay_transformations: false
                    },
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
    let data_path = harness.temp_dir_path().join("data");

    ParquetWriterHelper::from_record_batch(&data_path, &record_batch).unwrap();
    let data_logical_hash =
        kamu_data_utils::data::hash::get_parquet_logical_hash(&data_path).unwrap();
    let data_physical_hash =
        kamu_data_utils::data::hash::get_file_physical_hash(&data_path).unwrap();

    // Commit data
    let head = bar
        .dataset
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
        bar.dataset.as_metadata_chain().get_block(&head).await.unwrap(),
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
        harness
            .verification_svc
            .verify(
                VerificationRequest {
                    target: ResolvedDataset::from(&bar),
                    block_range: (None, None),
                    options: VerificationOptions {
                        check_integrity: true,
                        check_logical_hashes: true,
                        replay_transformations: false
                    },
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
        bar.dataset
            .as_data_repo()
            .get_internal_url(&data_physical_hash)
            .await,
    )
    .unwrap();

    ParquetWriterHelper::from_record_batch(&local_data_path, &record_batch).unwrap();

    // Check verification fails
    assert_matches!(
        harness.verification_svc.verify(
            VerificationRequest {
                target: ResolvedDataset::from(&bar),
                block_range: (None, None),
                options: VerificationOptions {
                    check_integrity: true,
                    check_logical_hashes: true,
                    replay_transformations: false
                },
            },
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

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct VerifyHarness {
    base_repo_harness: BaseRepoHarness,
    verification_svc: Arc<dyn VerificationService>,
}

impl VerifyHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant, None);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformExecutorImpl>()
            .add::<EngineProvisionerNull>()
            .add::<VerificationServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            verification_svc: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
