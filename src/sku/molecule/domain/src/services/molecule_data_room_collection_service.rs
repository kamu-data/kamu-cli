// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use internal_error::InternalError;
use kamu_datasets::{CollectionEntry, CollectionEntryListing, CollectionPath};
use nonempty::NonEmpty;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeDataRoomCollectionService: Send + Sync {
    async fn get_data_room_collection_entries(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<GetDataRoomCollectionEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionReadError>;

    async fn find_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError>;

    async fn find_data_room_collection_entry_by_ref(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError>;

    async fn upsert_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        r#ref: odf::DatasetID,
        extra_data: kamu_datasets::ExtraDataFields,
    ) -> Result<CollectionEntry, MoleculeDataRoomCollectionWriteError>;

    async fn move_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path_from: CollectionPath,
        path_to: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeDataRoomCollectionWriteError>;

    async fn remove_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeDataRoomCollectionWriteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct GetDataRoomCollectionEntriesFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
}

impl From<GetDataRoomCollectionEntriesFilters> for Option<kamu_datasets::ExtraDataFieldsFilter> {
    fn from(value: GetDataRoomCollectionEntriesFilters) -> Self {
        use kamu_datasets::ExtraDataFieldFilter as Filter;

        let maybe_tags_filter = value.by_tags.and_then(|values| {
            NonEmpty::from_vec(values).map(|values| Filter {
                field_name: "tags".to_string(),
                values,
            })
        });
        let maybe_categories_filter = value.by_categories.and_then(|values| {
            NonEmpty::from_vec(values).map(|values| Filter {
                field_name: "categories".to_string(),
                values,
            })
        });
        let maybe_access_levels_filter = value.by_access_levels.and_then(|values| {
            NonEmpty::from_vec(values).map(|values| Filter {
                field_name: "access_levels".to_string(),
                values,
            })
        });

        let filters = maybe_tags_filter
            .into_iter()
            .chain(maybe_categories_filter.into_iter())
            .chain(maybe_access_levels_filter.into_iter())
            .collect::<Vec<_>>();

        NonEmpty::from_vec(filters)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeUpdateDataRoomEntryResult {
    Success(MoleculeUpdateDataRoomEntrySuccess),
    UpToDate,
    EntryNotFound(CollectionPath),
}

#[derive(Debug)]
pub struct MoleculeUpdateDataRoomEntrySuccess {
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub inserted_records: Vec<(
        odf::metadata::OperationType,
        kamu_datasets::CollectionEntryRecord,
    )>,
    pub system_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDataRoomCollectionReadError {
    #[error(transparent)]
    DataRoomNotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDataRoomCollectionWriteError {
    #[error(transparent)]
    DataRoomNotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
