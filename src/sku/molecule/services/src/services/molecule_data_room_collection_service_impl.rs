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
    CollectionUpdateOperation,
    FindCollectionEntriesError,
    FindCollectionEntriesUseCase,
    ReadCheckedDataset,
    ResolvedDataset,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    ViewCollectionEntriesError,
    ViewCollectionEntriesUseCase,
    WriteCheckedDataset,
};
use kamu_molecule_domain::{
    MoleculeDataRoomCollectionReadError,
    MoleculeDataRoomCollectionService,
    MoleculeDataRoomCollectionWriteError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDataRoomCollectionService)]
pub struct MoleculeDataRoomCollectionServiceImpl {
    view_collection_entries: Arc<dyn ViewCollectionEntriesUseCase>,
    find_collection_entries: Arc<dyn FindCollectionEntriesUseCase>,
    update_collection_entries: Arc<dyn UpdateCollectionEntriesUseCase>,

    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl MoleculeDataRoomCollectionServiceImpl {
    async fn readable_data_room(
        &self,
        data_room_dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, MoleculeDataRoomCollectionReadError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &data_room_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => {
                    MoleculeDataRoomCollectionReadError::NotFound(e)
                }
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeDataRoomCollectionReadError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(readable_dataset)
    }

    async fn writable_data_room(
        &self,
        data_room_dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, MoleculeDataRoomCollectionWriteError> {
        let writable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &data_room_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Write,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => {
                    MoleculeDataRoomCollectionWriteError::NotFound(e)
                }
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeDataRoomCollectionWriteError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(writable_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MoleculeDataRoomCollectionService for MoleculeDataRoomCollectionServiceImpl {
    async fn get_data_room_collection_entries(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        // TODO: extra data filters
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionReadError> {
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
                    MoleculeDataRoomCollectionReadError::Access(e)
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
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError> {
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

    async fn get_data_room_collection_entry_by_ref(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let maybe_entry = self
            .find_collection_entries
            .execute_find_by_ref(ReadCheckedDataset(&readable_data_room), as_of, &[r#ref])
            .await
            .map_err(|e| match e {
                e @ FindCollectionEntriesError::Internal(_) => e.int_err(),
            })?;

        Ok(maybe_entry)
    }

    async fn upsert_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        path: CollectionPath,
        r#ref: odf::DatasetID,
        extra_data: kamu_datasets::ExtraDataFields,
    ) -> Result<(), MoleculeDataRoomCollectionWriteError> {
        let writable_data_room = self.writable_data_room(data_room_dataset_id).await?;

        match self
            .update_collection_entries
            .execute(
                WriteCheckedDataset(&writable_data_room),
                vec![CollectionUpdateOperation::add(path, r#ref, extra_data)],
                None,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(_)) => Ok(()),
            Ok(
                UpdateCollectionEntriesResult::UpToDate
                | UpdateCollectionEntriesResult::NotFound(_),
            ) => {
                unreachable!()
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(e)) => Err(e.into()),
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
