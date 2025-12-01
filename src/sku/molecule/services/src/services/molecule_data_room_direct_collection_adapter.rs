// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{
    CollectionEntry,
    CollectionEntryListing,
    CollectionPath,
    FindCollectionEntriesError,
    FindCollectionEntriesUseCase,
    ReadCheckedDataset,
    ResolvedDataset,
    ViewCollectionEntriesError,
    ViewCollectionEntriesUseCase,
};
use kamu_molecule_domain::{
    MoleculeDataRoomCollectionService,
    MoleculeDataRoomCollectionServiceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDataRoomCollectionService)]
pub struct MoleculeDataRoomDirectCollectionAdapter {
    view_collection_entries: Arc<dyn ViewCollectionEntriesUseCase>,
    find_collection_entries: Arc<dyn FindCollectionEntriesUseCase>,
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl MoleculeDataRoomDirectCollectionAdapter {
    async fn readable_data_room(
        &self,
        data_room_dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, MoleculeDataRoomCollectionServiceError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &data_room_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => {
                    MoleculeDataRoomCollectionServiceError::NotFound(e)
                }
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeDataRoomCollectionServiceError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(readable_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MoleculeDataRoomCollectionService for MoleculeDataRoomDirectCollectionAdapter {
    async fn get_data_room_collection_entries(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        // TODO: extra data filters
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionServiceError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let entries_listing = self
            .view_collection_entries
            .execute(
                ReadCheckedDataset(&readable_data_room),
                as_of,
                path_prefix,
                max_depth,
                pagination,
            )
            .await
            .map_err(|e| match e {
                ViewCollectionEntriesError::Access(e) => {
                    MoleculeDataRoomCollectionServiceError::Access(e)
                }
                e @ ViewCollectionEntriesError::Internal(_) => e.int_err().into(),
            })?;

        Ok(entries_listing)
    }

    async fn get_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
        // TODO: extra data filters
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionServiceError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let maybe_entry = self
            .find_collection_entries
            .execute_find_by_path(ReadCheckedDataset(&readable_data_room), as_of, path)
            .await
            .map_err(|e| match e {
                e @ FindCollectionEntriesError::Internal(_) => e.int_err(),
            })?;

        Ok(maybe_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
