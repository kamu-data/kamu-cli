// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::ErrorIntoInternal;
use kamu_datasets::CollectionPath;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUpsertDataRoomEntryUseCase)]
pub struct MoleculeUpsertDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeUpsertDataRoomEntryUseCase for MoleculeUpsertDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeUpsertDataRoomEntryUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, %path, %reference)
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        reference: odf::DatasetID,
        denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
    ) -> Result<MoleculeDataRoomEntry, MoleculeUpsertDataRoomEntryError> {
        let entry = self
            .data_room_collection_service
            .upsert_data_room_collection_entry(
                &molecule_project.data_room_dataset_id,
                source_event_time,
                path,
                reference,
                denormalized_latest_file_info.to_collection_extra_data_fields(),
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e) => {
                    MoleculeUpsertDataRoomEntryError::Internal(e.int_err())
                }
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeUpsertDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeUpsertDataRoomEntryError::Access(e)
                }
                e @ MoleculeDataRoomCollectionWriteError::Internal(_) => {
                    MoleculeUpsertDataRoomEntryError::Internal(e.int_err())
                }
            })?;

        // TODO: outbox event

        Ok(MoleculeDataRoomEntry {
            system_time: entry.system_time,
            event_time: entry.event_time,
            path: entry.path,
            reference: entry.reference,
            denormalized_latest_file_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
