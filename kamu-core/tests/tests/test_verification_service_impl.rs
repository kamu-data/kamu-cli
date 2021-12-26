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

#[test]
fn test_verify_data_consistency() {
    let tempdir = tempfile::tempdir().unwrap();

    let dataset_name = DatasetName::new_unchecked("bar");
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = Arc::new(VolumeLayout::new(&workspace_layout.local_volume_dir));
    let dataset_layout = DatasetLayout::create(&volume_layout, &dataset_name).unwrap();

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));

    let verification_svc = Arc::new(VerificationServiceImpl::new(
        metadata_repo.clone(),
        Arc::new(TestTransformService::new(Arc::new(Mutex::new(Vec::new())))),
        volume_layout.clone(),
    ));

    metadata_repo
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .source(MetadataFactory::dataset_source_root().build())
                .build(),
        )
        .unwrap();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name(&dataset_name)
        .source(MetadataFactory::dataset_source_deriv(["foo"]).build())
        .build();

    let (_hdl, head) = metadata_repo.add_dataset(dataset_snapshot).unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_name.as_local_ref(),
            (None, None),
            VerificationOptions {
                check_integrity: true,
                replay_transformations: false
            },
            None,
        ),
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

    write_record_batch_to_parquet(&data_path, &record_batch).unwrap();
    let data_logical_hash = data_utils::get_parquet_logical_hash(&data_path).unwrap();
    let data_physical_hash = data_utils::get_parquet_physical_hash(&data_path).unwrap();

    // "Commit" data
    let mut metadata_chain = metadata_repo
        .get_metadata_chain(&dataset_name.as_local_ref())
        .unwrap();
    let head = metadata_chain.append(
        MetadataFactory::metadata_block()
            .prev(&head)
            .output_slice(OutputSlice {
                data_logical_hash: data_logical_hash.clone(),
                data_physical_hash,
                data_interval: OffsetInterval { start: 0, end: 0 },
            })
            .build(),
    );
    std::fs::rename(data_path, dataset_layout.data_dir.join(head.to_string())).unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_name.as_local_ref(),
            (None, None),
            VerificationOptions {
                check_integrity: true,
                replay_transformations: false
            },
            None,
        ),
        Ok(VerificationResult::Valid)
    );

    // Overwrite with different data
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "f", "e"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
    write_record_batch_to_parquet(
        &dataset_layout.data_dir.join(head.to_string()),
        &record_batch,
    )
    .unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_name.as_local_ref(),
            (None, None),
            VerificationOptions {check_integrity: true, replay_transformations: false},
            None,
        ),
        Err(VerificationError::DataDoesNotMatchMetadata(
            DataDoesNotMatchMetadata {
                block_hash,
                logical_hash: Some(logical_hash),
                physical_hash: None
            }
        )) if block_hash == head && logical_hash.expected == data_logical_hash,
    );
}

fn write_record_batch_to_parquet(
    path: &Path,
    record_batch: &RecordBatch,
) -> Result<(), ParquetError> {
    use datafusion::parquet::arrow::ArrowWriter;

    let mut arrow_writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        record_batch.schema(),
        None,
    )?;

    arrow_writer.write(&record_batch)?;
    arrow_writer.close()?;
    Ok(())
}
