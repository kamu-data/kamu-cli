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
use kamu_datasets::{CollectionPath, CollectionPathV2};
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::{MoleculeDataRoomCollectionService, MoleculeDataRoomCollectionWriteError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUpdateDataRoomEntryUseCase)]
pub struct MoleculeUpdateDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeUpdateDataRoomEntryUseCase for MoleculeUpdateDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeUpdateDataRoomEntryUseCaseImpl_execute,
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
        content_text: Option<&str>,
    ) -> Result<MoleculeDataRoomEntry, MoleculeUpdateDataRoomEntryError> {
        let result = self
            .data_room_collection_service
            .move_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                source_event_time,
                path.clone(),
                path.clone(),
                Some(denormalized_latest_file_info.to_collection_extra_data_fields()),
                None,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e) => {
                    MoleculeUpdateDataRoomEntryError::Internal(e.int_err())
                }
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeUpdateDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeUpdateDataRoomEntryError::Access(e)
                }
                e @ MoleculeDataRoomCollectionWriteError::Internal(_) => {
                    MoleculeUpdateDataRoomEntryError::Internal(e.int_err())
                }
            })?;

        let data_room_entry = match result {
            MoleculeUpdateDataRoomEntryResult::Success(success) => MoleculeDataRoomEntry {
                system_time: success.system_time,
                event_time: source_event_time.unwrap_or(success.system_time),
                // SAFETY: All paths should be normalized after v2 migration
                path: CollectionPathV2::from_v1_unchecked(path),
                reference,
                denormalized_latest_file_info,
            },
            MoleculeUpdateDataRoomEntryResult::UpToDate => {
                // TODO: Return existing entry in `UpToDate` result
                let now = Utc::now();

                MoleculeDataRoomEntry {
                    system_time: now,
                    event_time: now,
                    // SAFETY: All paths should be normalized after v2 migration
                    path: CollectionPathV2::from_v1_unchecked(path),
                    reference,
                    denormalized_latest_file_info,
                }
            }
            MoleculeUpdateDataRoomEntryResult::EntryNotFound(_) => {
                // TODO: Entry presence is currently validated in GQL layer, but likely should
                // be moved into this use case
                return Err(format!("Entry not found with path: {path}")
                    .int_err()
                    .into());
            }
        };

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
                MoleculeDataRoomMessage::updated(
                    data_room_entry.system_time,
                    molecule_subject.account_id.clone(),
                    molecule_project.account_id.clone(),
                    molecule_project.ipnft_uid.clone(),
                    data_room_entry.clone(),
                    content_text.map(ToOwned::to_owned),
                ),
            )
            .await
            .int_err()?;

        Ok(data_room_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
