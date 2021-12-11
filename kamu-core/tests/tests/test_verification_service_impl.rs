use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::MetadataFactory;
use opendatafabric::*;

use super::test_pull_service_impl::TestTransformService;

#[test]
fn test_verify_data_consistency() {
    let tempdir = tempfile::tempdir().unwrap();

    let dataset_id = DatasetID::new_unchecked("foo");
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = Arc::new(VolumeLayout::new(&workspace_layout.local_volume_dir));
    let dataset_layout = DatasetLayout::create(&volume_layout, dataset_id).unwrap();

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));

    let verification_svc = Arc::new(VerificationServiceImpl::new(
        metadata_repo.clone(),
        Arc::new(TestTransformService::new(Arc::new(Mutex::new(Vec::new())))),
        volume_layout.clone(),
    ));

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .id(dataset_id)
        .source(MetadataFactory::dataset_source_root().build())
        .build();

    let head = metadata_repo.add_dataset(dataset_snapshot).unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_id,
            (None, None),
            VerificationOptions::default(),
            None,
        ),
        Ok(VerificationResult::Valid { blocks_verified: 0 })
    );

    let mut metadata_chain = metadata_repo.get_metadata_chain(&dataset_id).unwrap();

    let data_logical_hash =
        Multihash::from_multibase_str("zW1diShRuwkajoPuAGcexm1LU71yey6DahGex9TK1YfKp71").unwrap();
    // For now we don't verify physical hashes
    let data_physical_hash = data_logical_hash.clone();

    let head = metadata_chain.append(
        MetadataFactory::metadata_block()
            .prev(&head)
            .output_slice(OutputSlice {
                data_logical_hash,
                data_physical_hash,
                data_interval: OffsetInterval { start: 0, end: 0 },
            })
            .build(),
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
    write_record_batch_to_parquet(
        &dataset_layout.data_dir.join(head.to_string()),
        &record_batch,
    )
    .unwrap();

    assert_matches!(
        verification_svc.verify(
            &dataset_id,
            (None, None),
            VerificationOptions::default(),
            None,
        ),
        Ok(VerificationResult::Valid { blocks_verified: 1 })
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
            &dataset_id,
            (None, None),
            VerificationOptions::default(),
            None,
        ),
        Err(VerificationError::DataDoesNotMatchMetadata(
            DataDoesNotMatchMetadata {
                block_hash,
                data_logical_hash_expected,
                data_logical_hash_actual: _,
            }
        )) if block_hash == head && data_logical_hash_expected == data_logical_hash.to_string(),
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
