// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use file_utils::OwnedFile;
use kamu::domain::*;
use kamu::testing::{BaseRepoHarness, ParquetWriterHelper};
use kamu::*;
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_data_consistency() {
    let harness = VerifyHarness::new();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let foo = harness.create_root_dataset(&foo_alias).await;
    foo.dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let bar = harness
        .create_derived_dataset(&bar_alias, vec![foo_alias.as_local_ref()])
        .await;
    bar.dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    assert_matches!(
        harness
            .verification_svc
            .verify(
                VerificationRequest {
                    target: ResolvedDataset::from_created(&bar),
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
    let data_logical_hash = odf::utils::data::hash::get_parquet_logical_hash(&data_path).unwrap();
    let data_physical_hash = odf::utils::data::hash::get_file_physical_hash(&data_path).unwrap();

    // Commit data
    let head = bar
        .dataset
        .commit_add_data(
            odf::dataset::AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(odf::metadata::OffsetInterval { start: 0, end: 0 }),
                new_linked_objects: None,
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(data_path)),
            None,
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap()
        .new_head;

    assert_matches!(
        bar.dataset.as_metadata_chain().get_block(&head).await.unwrap(),
        odf::MetadataBlock {
            event: odf::MetadataEvent::AddData(odf::metadata::AddData {
                new_data: Some(odf::DataSlice {
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
                    target: ResolvedDataset::from_created(&bar),
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

    let local_data_path = odf::utils::data::local_url::into_local_path(
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
                target: ResolvedDataset::from_created(&bar),
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

#[tokio::test]
async fn test_verify_checkpoint_of_block_without_data() {
    let harness = VerifyHarness::new();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&foo_alias).await;

    let (head, checkpoint_hash) = harness
        .commit_checkpoint_without_data(&foo, b"checkpoint")
        .await;

    assert_matches!(
        harness
            .verify_integrity(ResolvedDataset::from_created(&foo))
            .await,
        VerificationResult {
            outcome: Ok(()),
            ..
        }
    );

    harness
        .overwrite_checkpoint(&foo, &checkpoint_hash, b"corrupted checkpoint")
        .await;

    assert_matches!(
        harness.verify_integrity(ResolvedDataset::from_created(&foo)).await,
        VerificationResult {
            outcome: Err(VerificationError::CheckpointDoesNotMatchMetadata(
                CheckpointDoesNotMatchMetadata {
                    block_hash,
                    error: CheckpointVerificationError::SizeMismatch { .. },
                }
            )),
            ..
        } if block_hash == head
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
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<TransformPlannerImpl>()
            .add::<TransformExecutorImpl>()
            .add::<EngineProvisionerNull>()
            .add::<VerificationServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            verification_svc: catalog.get_one().unwrap(),
        }
    }

    async fn verify_integrity(&self, target: ResolvedDataset) -> VerificationResult {
        self.verification_svc
            .verify(
                VerificationRequest {
                    target,
                    block_range: (None, None),
                    options: VerificationOptions {
                        check_integrity: true,
                        check_logical_hashes: true,
                        replay_transformations: false,
                    },
                },
                None,
            )
            .await
    }

    /// Commits an `AddData` block that carries only a checkpoint with
    /// `contents`; returns the new head and the checkpoint's physical hash
    async fn commit_checkpoint_without_data(
        &self,
        created: &CreateDatasetResult,
        contents: &[u8],
    ) -> (odf::Multihash, odf::Multihash) {
        let checkpoint_path = self.temp_dir_path().join("checkpoint");
        std::fs::write(&checkpoint_path, contents).unwrap();
        let checkpoint_hash =
            odf::utils::data::hash::get_file_physical_hash(&checkpoint_path).unwrap();

        let head = created
            .dataset
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset: None,
                    new_offset_interval: None,
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: None,
                },
                None,
                Some(odf::dataset::CheckpointRef::New(OwnedFile::new(
                    checkpoint_path,
                ))),
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap()
            .new_head;

        (head, checkpoint_hash)
    }

    async fn overwrite_checkpoint(
        &self,
        created: &CreateDatasetResult,
        checkpoint_hash: &odf::Multihash,
        contents: &[u8],
    ) {
        let path = odf::utils::data::local_url::into_local_path(
            created
                .dataset
                .as_checkpoint_repo()
                .get_internal_url(checkpoint_hash)
                .await,
        )
        .unwrap();
        std::fs::write(path, contents).unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
