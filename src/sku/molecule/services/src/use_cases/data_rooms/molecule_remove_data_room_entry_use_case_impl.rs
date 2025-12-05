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
use messaging_outbox::{Outbox, OutboxExt};

use crate::domain::*;

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
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeRemoveDataRoomEntryError> {
        let result = self
            .data_room_collection_service
            .remove_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                source_event_time,
                path.clone(),
                expected_head,
            )
            .await
            .map_err(|e| match e {
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
            })?;

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
