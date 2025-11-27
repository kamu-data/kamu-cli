// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::{fs, io};

use datafusion::arrow::array::{Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use kamu::testing::ParquetWriterHelper;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use odf::dataset::DatasetLayout;
use odf::metadata::testing::{AddDataBuilder, MetadataFactory};
use odf::storage::ObjectRepository as _;
use odf::storage::lfs::ObjectRepositoryLocalFSSha3;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn copy_folder_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    if src.exists() {
        fs::create_dir_all(dst)?;
        let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
        fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn copy_dataset_files(
    src_layout: &DatasetLayout,
    dst_layout: &DatasetLayout,
) -> io::Result<()> {
    // Don't copy `info`
    copy_folder_recursively(&src_layout.blocks_dir, &dst_layout.blocks_dir)?;
    copy_folder_recursively(&src_layout.checkpoints_dir, &dst_layout.checkpoints_dir)?;
    copy_folder_recursively(&src_layout.data_dir, &dst_layout.data_dir)?;
    copy_folder_recursively(&src_layout.refs_dir, &dst_layout.refs_dir)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn write_dataset_alias(dataset_layout: &DatasetLayout, alias: &odf::DatasetAlias) {
    if !dataset_layout.info_dir.is_dir() {
        std::fs::create_dir_all(dataset_layout.info_dir.clone()).unwrap();
    }

    use tokio::io::AsyncWriteExt;

    let alias_path = dataset_layout.info_dir.join("alias");
    let mut alias_file = tokio::fs::File::create(alias_path).await.unwrap();

    alias_file
        .write_all(alias.to_string().as_bytes())
        .await
        .unwrap();
    alias_file.flush().await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_random_data(
    dataset_layout: &DatasetLayout,
    prev_offset: Option<u64>,
    prev_checkpoint: Option<odf::Multihash>,
) -> AddDataBuilder {
    let (d_hash, d_size) = create_random_parquet_file(
        &dataset_layout.data_dir,
        10,
        prev_offset.map_or(0, |offset| offset + 1),
    )
    .await;

    let (c_hash, c_size) = create_random_file(&dataset_layout.checkpoints_dir).await;

    MetadataFactory::add_data()
        .prev_checkpoint(prev_checkpoint)
        .prev_offset(prev_offset)
        .new_data_physical_hash(d_hash)
        .new_data_size(d_size as u64)
        .new_checkpoint_physical_hash(c_hash)
        .new_checkpoint_size(c_size as u64)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_random_file(root: &Path) -> (odf::Multihash, usize) {
    use rand::RngCore;

    let mut data = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut data);

    std::fs::create_dir_all(root).unwrap();

    let repo = ObjectRepositoryLocalFSSha3::new(root);
    let hash = repo
        .insert_bytes(&data, odf::storage::InsertOpts::default())
        .await
        .unwrap()
        .hash;

    (hash, data.len())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_random_parquet_file(
    root: &Path,
    num_records: usize,
    start_offset: u64,
) -> (odf::Multihash, usize) {
    use rand::RngCore;

    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("a", DataType::UInt64, false),
    ]));

    let mut column_a: Vec<u64> = Vec::with_capacity(num_records);
    let mut column_offset: Vec<u64> = Vec::with_capacity(num_records);
    for index in 0..num_records {
        column_a.push(rand::thread_rng().next_u64());
        column_offset.push(start_offset + (index as u64));
    }

    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(column_a));
    let offset: Arc<dyn Array> = Arc::new(UInt64Array::from(column_offset));
    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&a), Arc::clone(&offset)],
    )
    .unwrap();

    std::fs::create_dir_all(root).unwrap();
    let tmp_data_path = root.join("data");
    ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

    let repo = ObjectRepositoryLocalFSSha3::new(root);
    let hash = repo
        .insert_file_move(&tmp_data_path, odf::storage::InsertOpts::default())
        .await
        .unwrap()
        .hash;

    (hash, num_records)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn commit_add_data_event(
    dataset_registry: &dyn DatasetRegistry,
    dataset_ref: &odf::DatasetRef,
    dataset_layout: &DatasetLayout,
    prev_data_block_hash: Option<odf::Multihash>,
) -> odf::dataset::CommitResult {
    let resolved_dataset = dataset_registry
        .get_dataset_by_ref(dataset_ref)
        .await
        .unwrap();

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

    let random_data = create_random_data(dataset_layout, prev_offset, prev_checkpoint)
        .await
        .build();

    resolved_dataset
        .commit_event(
            odf::MetadataEvent::AddData(random_data),
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
