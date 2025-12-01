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
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionServiceError>;

    async fn get_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionServiceError>;

    async fn get_data_room_collection_entry_by_ref(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionServiceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDataRoomCollectionServiceError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
