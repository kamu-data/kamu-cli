// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ErrorIntoInternal;
use kamu_datasets::CollectionPath;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeFindDataRoomEntryUseCase)]
pub struct MoleculeFindDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeFindDataRoomEntryUseCase for MoleculeFindDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeFindDataRoomEntryUseCaseImpl_execute_find_by_path,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, as_of = ?as_of, path = %path)
    )]
    async fn execute_find_by_path(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<MoleculeDataRoomEntry>, MoleculeFindDataRoomEntryError> {
        let maybe_entry = self
            .data_room_collection_service
            .find_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                as_of,
                path,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionReadError::DataRoomNotFound(e) => e.int_err().into(),
                MoleculeDataRoomCollectionReadError::Access(e) => {
                    MoleculeFindDataRoomEntryError::Access(e)
                }
                MoleculeDataRoomCollectionReadError::Internal(e) => {
                    MoleculeFindDataRoomEntryError::Internal(e)
                }
            })?;

        let maybe_molecule_entry = maybe_entry.map(MoleculeDataRoomEntry::from_collection_entry);
        Ok(maybe_molecule_entry)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFindDataRoomEntryUseCaseImpl_execute_find_by_ref,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, as_of = ?as_of, r#ref = ?r#ref)
    )]
    async fn execute_find_by_ref(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<MoleculeDataRoomEntry>, MoleculeFindDataRoomEntryError> {
        let maybe_entry = self
            .data_room_collection_service
            .find_data_room_collection_entry_by_ref(
                &molecule_project.data_room_dataset_id,
                as_of,
                r#ref,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionReadError::DataRoomNotFound(e) => e.int_err().into(),
                MoleculeDataRoomCollectionReadError::Access(e) => {
                    MoleculeFindDataRoomEntryError::Access(e)
                }
                MoleculeDataRoomCollectionReadError::Internal(e) => {
                    MoleculeFindDataRoomEntryError::Internal(e)
                }
            })?;

        let maybe_molecule_entry = maybe_entry.map(MoleculeDataRoomEntry::from_collection_entry);
        Ok(maybe_molecule_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
