// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use file_utils::OwnedFile;
use kamu::testing::ParquetWriterHelper;
use kamu_datasets::DatasetRegistry;
use odf::dataset::{AddDataParams, CheckpointRef};
use odf::metadata::OffsetInterval;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const PROTOCOL_TRANSFER_SUBDIRS: [&str; 4] = ["blocks", "checkpoints", "data", "refs"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_random_file(path: PathBuf) -> OwnedFile {
    use rand::RngCore;

    let mut data = [0u8; 32];
    rand::rng().fill_bytes(&mut data);

    std::fs::write(&path, data).unwrap();

    OwnedFile::new(path)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_random_parquet_file(path: PathBuf, offset_interval: &OffsetInterval) -> OwnedFile {
    use rand::RngCore;

    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("a", DataType::UInt64, false),
    ]));

    let mut column_a: Vec<u64> = Vec::with_capacity(offset_interval.len());
    let mut column_offset: Vec<u64> = Vec::with_capacity(offset_interval.len());
    for index in 0..offset_interval.len() {
        column_a.push(rand::rng().next_u64());
        column_offset.push(offset_interval.start + (index as u64));
    }

    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(column_a));
    let offset: Arc<dyn Array> = Arc::new(UInt64Array::from(column_offset));
    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&a), Arc::clone(&offset)],
    )
    .unwrap();

    ParquetWriterHelper::from_record_batch(&path, &record_batch).unwrap();

    OwnedFile::new(path)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn commit_add_data_event(
    dataset_registry: &dyn DatasetRegistry,
    dataset_handle: &odf::DatasetHandle,
    prev_data_block_hash: Option<odf::Multihash>,
) -> odf::dataset::CommitResult {
    let tempdir = tempfile::tempdir().unwrap();

    let resolved_dataset = dataset_registry.get_dataset_by_handle(dataset_handle).await;

    let (prev_offset, prev_checkpoint) = if let Some(prev_data_block_hash) = prev_data_block_hash {
        let prev_data_block = resolved_dataset
            .as_metadata_chain()
            .get_block(&prev_data_block_hash)
            .await
            .unwrap();
        match prev_data_block.event {
            odf::MetadataEvent::AddData(add_data) => (
                Some(add_data.new_data.unwrap().offset_interval.end),
                Some(add_data.new_checkpoint.unwrap().physical_hash),
            ),
            _ => panic!("unexpected data event type"),
        }
    } else {
        (None, None)
    };

    let num_records = 10;
    let new_offset_interval = OffsetInterval {
        start: prev_offset.map_or(0, |offset| offset + 1),
        end: prev_offset.map_or(0, |offset| offset + 1) + num_records - 1,
    };

    let data = create_random_parquet_file(tempdir.path().join("data"), &new_offset_interval);

    let checkpoint = create_random_file(tempdir.path().join("checkpoint"));

    let add_data = AddDataParams {
        prev_checkpoint,
        prev_offset,
        new_offset_interval: Some(new_offset_interval),
        new_linked_objects: None,
        new_watermark: None,
        new_source_state: None,
    };

    resolved_dataset
        .commit_add_data(
            add_data,
            Some(data),
            Some(CheckpointRef::New(checkpoint)),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_dataset_ref(
    account_name: Option<&odf::AccountName>,
    dataset_name: &str,
) -> odf::DatasetRef {
    match account_name {
        Some(account_name) => {
            odf::DatasetRef::from_str(format!("{account_name}/{dataset_name}").as_str()).unwrap()
        }
        None => odf::DatasetRef::from_str(dataset_name).unwrap(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
