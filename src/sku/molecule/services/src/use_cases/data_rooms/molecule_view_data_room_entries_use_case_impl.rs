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
use kamu_molecule_domain::*;

use crate::{MoleculeDataRoomCollectionReadError, MoleculeDataRoomCollectionService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewDataRoomEntriesUseCase)]
pub struct MoleculeViewDataRoomEntriesUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewDataRoomEntriesUseCase for MoleculeViewDataRoomEntriesUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeViewDataRoomEntriesUseCaseImpl_execute,
        skip_all,
        fields(
            ipnft_uid = %molecule_project.ipnft_uid,
            as_of = ?as_of,
            path_prefix = ?path_prefix,
            max_depth = ?max_depth,
            pagination = ?pagination
        )
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewDataRoomEntriesError> {
        let entries_listing = self
            .data_room_collection_service
            .get_data_room_collection_entries(
                &molecule_project.data_room_dataset_id,
                as_of,
                path_prefix,
                max_depth,
                filters,
                pagination,
            )
            .await
            .map_err(|e| -> MoleculeViewDataRoomEntriesError {
                use MoleculeDataRoomCollectionReadError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::DataRoomNotFound(_) | E::Internal(_) => e.int_err().into(),
                }
            })?;

        let molecule_entries = entries_listing
            .list
            .into_iter()
            .map(MoleculeDataRoomEntry::from_collection_entry)
            .collect();

        Ok(MoleculeDataRoomEntriesListing {
            total_count: entries_listing.total_count,
            list: molecule_entries,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
