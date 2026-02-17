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

use crate::{
    MoleculeDataRoomCollectionReadError,
    MoleculeDataRoomCollectionService,
    MoleculeDataRoomCollectionWriteError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeRemoveDataRoomEntryUseCase)]
pub struct MoleculeRemoveDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
    outbox: Arc<dyn Outbox>,
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
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        change_by: String,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeRemoveDataRoomEntryError> {
        // Inspect latest entry to see if we need to correct change_by before retracting
        let (updated_extra_data, needs_correction): (Option<kamu_datasets::ExtraDataFields>, bool) =
            self.data_room_collection_service
                .find_data_room_collection_entry_by_path(
                    &molecule_project.data_room_dataset_id,
                    None,
                    path.clone(),
                )
                .await
                .map_err(MoleculeRemoveDataRoomEntryError::from)?
                .map(|entry| -> Result<_, MoleculeRemoveDataRoomEntryError> {
                    let mut denorm = MoleculeDenormalizeFileToDataRoom::try_from_extra_data_fields(
                        entry.extra_data,
                    )
                    .int_err()?;
                    let needs_correction = denorm.change_by != change_by;
                    denorm.change_by.clone_from(&change_by);
                    Ok((
                        Some(denorm.to_collection_extra_data_fields()),
                        needs_correction,
                    ))
                })
                .transpose()?
                .unwrap_or((None, false));

        // If change_by differs, first write a correction (path move with updated
        // extra_data)
        let mut next_expected_head = expected_head.clone();
        if needs_correction {
            let extra_data = updated_extra_data.clone().expect("extra_data must exist");

            match self
                .data_room_collection_service
                .move_data_room_collection_entry_by_path(
                    &molecule_project.data_room_dataset_id,
                    source_event_time,
                    path.clone(),
                    path.clone(),
                    Some(extra_data),
                    next_expected_head.clone(),
                )
                .await?
            {
                MoleculeUpdateDataRoomEntryResult::Success(success) => {
                    next_expected_head = Some(success.new_head);
                }
                MoleculeUpdateDataRoomEntryResult::UpToDate
                | MoleculeUpdateDataRoomEntryResult::EntryNotFound(_) => {
                    // No-op, proceed with removal
                }
            }
        }

        let mut result = self
            .data_room_collection_service
            .remove_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                source_event_time,
                path.clone(),
                next_expected_head,
            )
            .await?;

        if let MoleculeUpdateDataRoomEntryResult::Success(success) = &mut result {
            for (_op, record) in &mut success.inserted_records {
                if let Some(extra_data) = updated_extra_data.clone() {
                    record.extra_data = extra_data;
                } else {
                    let mut denorm = MoleculeDenormalizeFileToDataRoom::try_from_extra_data_fields(
                        record.extra_data.clone(),
                    )
                    .int_err()?;
                    denorm.change_by.clone_from(&change_by);
                    record.extra_data = denorm.to_collection_extra_data_fields();
                }
            }
        }

        match result {
            MoleculeUpdateDataRoomEntryResult::Success(success) => {
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
                        MoleculeDataRoomMessage::removed(
                            success.system_time,
                            molecule_subject.account_id.clone(),
                            molecule_project.account_id.clone(),
                            molecule_project.ipnft_uid.clone(),
                            path,
                        ),
                    )
                    .await
                    .int_err()?;

                Ok(MoleculeUpdateDataRoomEntryResult::Success(success))
            }

            MoleculeUpdateDataRoomEntryResult::EntryNotFound(_) => {
                // Deletes are idempotent
                Ok(MoleculeUpdateDataRoomEntryResult::UpToDate)
            }

            MoleculeUpdateDataRoomEntryResult::UpToDate => {
                Ok(MoleculeUpdateDataRoomEntryResult::UpToDate)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<MoleculeDataRoomCollectionWriteError> for MoleculeRemoveDataRoomEntryError {
    fn from(value: MoleculeDataRoomCollectionWriteError) -> Self {
        match value {
            MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e) => e.int_err().into(),
            MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                MoleculeRemoveDataRoomEntryError::RefCASFailed(e)
            }
            MoleculeDataRoomCollectionWriteError::QuotaExceeded(e) => {
                MoleculeRemoveDataRoomEntryError::QuotaExceeded(e)
            }
            MoleculeDataRoomCollectionWriteError::Access(e) => {
                MoleculeRemoveDataRoomEntryError::Access(e)
            }
            MoleculeDataRoomCollectionWriteError::Internal(e) => {
                MoleculeRemoveDataRoomEntryError::Internal(e)
            }
        }
    }
}

impl From<MoleculeDataRoomCollectionReadError> for MoleculeRemoveDataRoomEntryError {
    fn from(value: MoleculeDataRoomCollectionReadError) -> Self {
        match value {
            MoleculeDataRoomCollectionReadError::DataRoomNotFound(e) => e.int_err().into(),
            MoleculeDataRoomCollectionReadError::Access(e) => {
                MoleculeRemoveDataRoomEntryError::Access(e)
            }
            MoleculeDataRoomCollectionReadError::Internal(e) => {
                MoleculeRemoveDataRoomEntryError::Internal(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
