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
use kamu_datasets::CollectionPath;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectDataRoomEntriesUseCase)]
pub struct MoleculeViewProjectDataRoomEntriesUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewProjectDataRoomEntriesUseCase for MoleculeViewProjectDataRoomEntriesUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeViewProjectDataRoomEntriesUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, as_of = ?as_of, path_prefix = ?path_prefix, max_depth = ?max_depth, pagination = ?pagination)
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        // TODO: filters
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewProjectDataRoomError> {
        let entries_listing = self
            .data_room_collection_service
            .get_data_room_collection_entries(
                &molecule_project.data_room_dataset_id,
                as_of,
                path_prefix,
                max_depth,
                pagination,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionReadError::DataRoomNotFound(e) => e.int_err().into(),
                MoleculeDataRoomCollectionReadError::Access(e) => {
                    MoleculeViewProjectDataRoomError::Access(e)
                }
                MoleculeDataRoomCollectionReadError::Internal(e) => {
                    MoleculeViewProjectDataRoomError::Internal(e)
                }
            })?;

        let molecule_entries = entries_listing
            .list
            .into_iter()
            .map(MoleculeDataRoomEntry::try_from_collection_entry)
            .try_collect()?;

        Ok(MoleculeDataRoomEntriesListing {
            total_count: entries_listing.total_count,
            list: molecule_entries,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
