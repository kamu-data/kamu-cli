// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use kamu::domain::*;
use kamu::infra::utils::data_utils;
use kamu::infra::*;
use kamu::testing::MetadataFactory;
use opendatafabric::*;

use super::test_pull_service_impl::TestTransformService;

#[tokio::test]
async fn test_verify_data_consistency() {
    let tempdir = tempfile::tempdir().unwrap();

    let dataset_name = DatasetName::new_unchecked("bar");
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = Arc::new(VolumeLayout::new(&workspace_layout.local_volume_dir));
    let dataset_layout = DatasetLayout::create(&volume_layout, &dataset_name).unwrap();

    let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));

    let verification_svc = Arc::new(VerificationServiceImpl::new(
        dataset_reg.clone(),
        Arc::new(TestTransformService::new(Arc::new(Mutex::new(Vec::new())))),
        volume_layout.clone(),
    ));

    dataset_reg
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .unwrap();

    let (_hdl, head) = dataset_reg
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name(&dataset_name)
                .kind(DatasetKind::Derivative)
                .push_event(MetadataFactory::set_transform(["foo"]).build())
                .build(),
        )
        .unwrap();

    assert_matches!(
        verification_svc
            .verify(
                &dataset_name.as_local_ref(),
                (None, None),
                VerificationOptions {
                    check_integrity: true,
                    replay_transformations: false
                },
                None,
            )
            .await,
        Ok(VerificationResult::Valid)
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

    let size = write_record_batch_to_parquet(&data_path, &record_batch).unwrap();
    let data_logical_hash = data_utils::get_parquet_logical_hash(&data_path).unwrap();
    let data_physical_hash = data_utils::get_file_physical_hash(&data_path).unwrap();

    // "Commit" data
    let mut metadata_chain = dataset_reg
        .get_metadata_chain(&dataset_name.as_local_ref())
        .unwrap();
    let head = metadata_chain.append(
        MetadataFactory::metadata_block(AddData {
            input_checkpoint: None,
            output_data: DataSlice {
                logical_hash: data_logical_hash.clone(),
                physical_hash: data_physical_hash.clone(),
                interval: OffsetInterval { start: 0, end: 0 },
                size: size as i64,
            },
            output_checkpoint: None,
            output_watermark: None,
        })
        .prev(&head)
        .build(),
    );
    std::fs::rename(
        data_path,
        dataset_layout
            .data_dir
            .join(data_physical_hash.to_multibase_string()),
    )
    .unwrap();

    assert_matches!(
        verification_svc
            .verify(
                &dataset_name.as_local_ref(),
                (None, None),
                VerificationOptions {
                    check_integrity: true,
                    replay_transformations: false
                },
                None,
            )
            .await,
        Ok(VerificationResult::Valid)
    );

    // Overwrite with different data
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "f", "e"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
    write_record_batch_to_parquet(
        &dataset_layout
            .data_dir
            .join(data_physical_hash.to_multibase_string()),
        &record_batch,
    )
    .unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_name.as_local_ref(),
            (None, None),
            VerificationOptions {check_integrity: true, replay_transformations: false},
            None,
        ).await,
        Err(VerificationError::DataDoesNotMatchMetadata(
            DataDoesNotMatchMetadata {
                block_hash,
                error: DataVerificationError::LogicalHashMismatch { expected, .. },
            }
        )) if block_hash == head && expected == data_logical_hash,
    );
}

fn write_record_batch_to_parquet(
    path: &Path,
    record_batch: &RecordBatch,
) -> Result<u64, ParquetError> {
    use datafusion::parquet::arrow::ArrowWriter;

    let mut arrow_writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        record_batch.schema(),
        None,
    )?;

    arrow_writer.write(&record_batch)?;
    arrow_writer.close()?;

    Ok(std::fs::metadata(path).unwrap().len())
}
