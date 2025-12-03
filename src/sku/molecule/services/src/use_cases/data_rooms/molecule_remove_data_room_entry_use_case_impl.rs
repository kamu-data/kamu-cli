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
#[dill::interface(dyn MoleculeRemoveDataRoomEntryUseCase)]
pub struct MoleculeRemoveDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeRemoveDataRoomEntryUseCase for MoleculeRemoveDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeRemoveDataRoomEntryUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, path = %path, ?expected_head)
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        path: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeRemoveDataRoomEntryError> {
        let result = self
            .data_room_collection_service
            .remove_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                path,
                expected_head,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e) => e.int_err().into(),
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeRemoveDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeRemoveDataRoomEntryError::Access(e)
                }
                MoleculeDataRoomCollectionWriteError::Internal(e) => {
                    MoleculeRemoveDataRoomEntryError::Internal(e)
                }
            })?;

        // TODO: outbox event

        match result {
            MoleculeUpdateDataRoomEntryResult::EntryNotFound(_) => {
                // Deletes are idempotent
                Ok(MoleculeUpdateDataRoomEntryResult::UpToDate)
            }
            _ => Ok(result),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
