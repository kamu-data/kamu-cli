// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use kamu_datasets::{CollectionEntry, CollectionEntryListing, CollectionPath};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeDataRoomCollectionService: Send + Sync {
    async fn get_data_room_collection_entries(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        // TODO: extra data filters
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionReadError>;

    async fn get_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError>;

    async fn get_data_room_collection_entry_by_ref(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError>;

    async fn upsert_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        path: CollectionPath,
        r#ref: odf::DatasetID,
        extra_data: kamu_datasets::ExtraDataFields,
    ) -> Result<(), MoleculeDataRoomCollectionWriteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDataRoomCollectionReadError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDataRoomCollectionWriteError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
