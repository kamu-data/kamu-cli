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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_datasets::CollectionPath;
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::{MoleculeDataRoomCollectionService, MoleculeDataRoomCollectionWriteError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeCreateDataRoomEntryUseCase)]
pub struct MoleculeCreateDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeCreateDataRoomEntryUseCase for MoleculeCreateDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeCreateDataRoomEntryUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, %path, %reference)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        reference: odf::DatasetID,
        denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
    ) -> Result<MoleculeDataRoomEntry, MoleculeCreateDataRoomEntryError> {
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
                    MoleculeCreateDataRoomEntryError::Internal(e.int_err())
                }
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeCreateDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeCreateDataRoomEntryError::Access(e)
                }
                e @ MoleculeDataRoomCollectionWriteError::Internal(_) => {
                    MoleculeCreateDataRoomEntryError::Internal(e.int_err())
                }
            })?;

        let data_room_entry = MoleculeDataRoomEntry {
            system_time: entry.system_time,
            event_time: entry.event_time,
            path: entry.path,
            reference: entry.reference,
            denormalized_latest_file_info,
        };

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
                MoleculeDataRoomMessage::created(
                    data_room_entry.system_time,
                    molecule_subject.account_id.clone(),
                    molecule_project.account_id.clone(),
                    molecule_project.ipnft_uid.clone(),
                    data_room_entry.clone(),
                ),
            )
            .await
            .int_err()?;

        Ok(data_room_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
