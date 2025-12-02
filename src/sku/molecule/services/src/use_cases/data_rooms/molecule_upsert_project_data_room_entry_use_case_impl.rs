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

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUpsertProjectDataRoomEntryUseCase)]
pub struct MoleculeUpsertProjectDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeUpsertProjectDataRoomEntryUseCase for MoleculeUpsertProjectDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeUpsertProjectDataRoomEntryUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, path = %molecule_data_room_entry.path)
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        molecule_data_room_entry: &MoleculeDataRoomEntry,
    ) -> Result<(), MoleculeUpsertProjectDataRoomEntryError> {
        self.data_room_collection_service
            .upsert_data_room_collection_entry(
                &molecule_project.data_room_dataset_id,
                molecule_data_room_entry.path.clone(),
                molecule_data_room_entry.reference.clone(),
                molecule_data_room_entry
                    .denormalized_latest_file_info
                    .to_collection_extra_data_fields(),
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionWriteError::NotFound(e) => {
                    MoleculeUpsertProjectDataRoomEntryError::Internal(e.int_err())
                }
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeUpsertProjectDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeUpsertProjectDataRoomEntryError::Access(e)
                }
                e @ MoleculeDataRoomCollectionWriteError::Internal(_) => {
                    MoleculeUpsertProjectDataRoomEntryError::Internal(e.int_err())
                }
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
