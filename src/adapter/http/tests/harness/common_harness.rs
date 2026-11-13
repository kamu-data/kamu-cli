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
use kamu_datasets::{
    CommitDatasetEventUseCase,
    DatasetReferenceService,
    DatasetRegistry,
    GetDatasetReferenceError,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use odf::dataset::{AddDataParams, CheckpointRef};
use odf::metadata::OffsetInterval;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const PROTOCOL_TRANSFER_SUBDIRS: [&str; 4] = ["blocks", "checkpoints", "data", "refs"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_random_file(path: PathBuf) -> OwnedFile {
    use rand::Rng;

    let mut data = [0u8; 32];
    rand::rng().fill_bytes(&mut data);

    std::fs::write(&path, data).unwrap();

    OwnedFile::new(path)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_random_parquet_file(path: PathBuf, offset_interval: &OffsetInterval) -> OwnedFile {
    use rand::Rng;

    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("a", DataType::UInt64, false),
    ]));

    let mut column_a: Vec<u64> = Vec::with_capacity(offset_interval.len());
    let mut column_offset: Vec<u64> = Vec::with_capacity(offset_interval.len());
    for index in 0..offset_interval.len() {
        column_a.push(rand::rng().next_u64());
        column_offset.push(offset_interval.start + u64::try_from(index).unwrap());
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

/// Writes blocks that no reference names, the way a writer that has not
/// moved `HEAD` yet leaves them
pub(crate) struct UnreferencedBlocks {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

impl UnreferencedBlocks {
    pub fn new(dataset_registry: Arc<dyn DatasetRegistry>) -> Self {
        Self { dataset_registry }
    }

    /// Appends a `SetInfo` block after the current `HEAD` without moving any
    /// reference. The block takes the `HEAD` block's system time, which keeps
    /// the chain monotonic whatever time the server's clock is set to.
    pub async fn append(&self, dataset_handle: &odf::DatasetHandle) -> odf::Multihash {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;
        let chain = resolved_dataset.as_metadata_chain();

        let head = chain.resolve_ref(&odf::BlockRef::Head).await.unwrap();
        let head_block = chain.get_block(&head).await.unwrap();

        let block = MetadataFactory::metadata_block(
            MetadataFactory::set_info()
                .description("concurrent")
                .build(),
        )
        .prev(&head, head_block.sequence_number)
        .system_time(head_block.system_time)
        .build();

        chain
            .append(
                block,
                odf::dataset::AppendOpts {
                    update_ref: None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits to datasets, moving `HEAD` as any writer would
pub(crate) struct DatasetCommits {
    commit_dataset_event: Arc<dyn CommitDatasetEventUseCase>,
}

impl DatasetCommits {
    pub fn new(commit_dataset_event: Arc<dyn CommitDatasetEventUseCase>) -> Self {
        Self {
            commit_dataset_event,
        }
    }

    /// Commits a `SetInfo` through the use case and returns the new head
    pub async fn commit_set_info(&self, dataset_handle: &odf::DatasetHandle) -> odf::Multihash {
        self.commit_dataset_event
            .execute(
                dataset_handle,
                MetadataFactory::set_info()
                    .description("committed")
                    .build()
                    .into(),
            )
            .await
            .unwrap()
            .new_head
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reads the stored `HEAD` of datasets
pub(crate) struct StoredHeads {
    dataset_reference_service: Arc<dyn DatasetReferenceService>,
}

impl StoredHeads {
    pub fn new(dataset_reference_service: Arc<dyn DatasetReferenceService>) -> Self {
        Self {
            dataset_reference_service,
        }
    }

    pub async fn get(&self, dataset_id: &odf::DatasetID) -> odf::Multihash {
        self.dataset_reference_service
            .get_reference(dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    }

    /// `None` while the dataset has no `HEAD`
    pub async fn find(&self, dataset_id: &odf::DatasetID) -> Option<odf::Multihash> {
        match self
            .dataset_reference_service
            .get_reference(dataset_id, &odf::BlockRef::Head)
            .await
        {
            Ok(head) => Some(head),
            Err(GetDatasetReferenceError::NotFound(_)) => None,
            Err(GetDatasetReferenceError::Internal(e)) => panic!("{e}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stores datasets with their own `Seed` and an entry but no `HEAD`, as an
/// interrupted creation leaves them
pub(crate) struct HeadlessDatasets {
    create_dataset_helper: Arc<CreateDatasetUseCaseHelper>,
}

impl HeadlessDatasets {
    pub fn new(create_dataset_helper: Arc<CreateDatasetUseCaseHelper>) -> Self {
        Self {
            create_dataset_helper,
        }
    }

    pub async fn create(
        &self,
        owner_account_id: &odf::AccountID,
        dataset_alias: &odf::DatasetAlias,
    ) -> odf::DatasetHandle {
        let seed_block = MetadataFactory::metadata_block(
            MetadataFactory::seed(odf::DatasetKind::Root)
                .id_from(dataset_alias.dataset_name.as_str())
                .build(),
        )
        .build_typed();
        let stored = self
            .create_dataset_helper
            .store_dataset(
                dataset_alias,
                seed_block,
                odf::dataset::StoreDatasetOpts::default(),
            )
            .await
            .unwrap();
        self.create_dataset_helper
            .create_dataset_entry(
                &stored.dataset_id,
                owner_account_id,
                dataset_alias,
                stored.dataset_kind,
            )
            .await
            .unwrap();

        odf::DatasetHandle::new(
            stored.dataset_id,
            dataset_alias.clone(),
            stored.dataset_kind,
        )
    }
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
        let odf::MetadataEvent::AddData(add_data) = prev_data_block.event else {
            panic!("unexpected data event type");
        };
        (
            Some(add_data.new_data.unwrap().offset_interval.end),
            Some(add_data.new_checkpoint.unwrap().physical_hash),
        )
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
