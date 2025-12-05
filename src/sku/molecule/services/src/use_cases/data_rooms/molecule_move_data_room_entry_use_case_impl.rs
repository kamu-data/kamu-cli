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
#[dill::interface(dyn MoleculeMoveDataRoomEntryUseCase)]
pub struct MoleculeMoveDataRoomEntryUseCaseImpl {
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeMoveDataRoomEntryUseCase for MoleculeMoveDataRoomEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeMoveDataRoomEntryUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, path_from = %path_from, path_to = %path_to, ?expected_head)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        path_from: CollectionPath,
        path_to: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeMoveDataRoomEntryError> {
        let result = self
            .data_room_collection_service
            .move_data_room_collection_entry_by_path(
                &molecule_project.data_room_dataset_id,
                source_event_time,
                path_from.clone(),
                path_to.clone(),
                expected_head,
            )
            .await
            .map_err(|e| match e {
                MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e) => e.int_err().into(),
                MoleculeDataRoomCollectionWriteError::RefCASFailed(e) => {
                    MoleculeMoveDataRoomEntryError::RefCASFailed(e)
                }
                MoleculeDataRoomCollectionWriteError::QuotaExceeded(e) => {
                    MoleculeMoveDataRoomEntryError::QuotaExceeded(e)
                }
                MoleculeDataRoomCollectionWriteError::Access(e) => {
                    MoleculeMoveDataRoomEntryError::Access(e)
                }
                MoleculeDataRoomCollectionWriteError::Internal(e) => {
                    MoleculeMoveDataRoomEntryError::Internal(e)
                }
            })?;

        if let MoleculeUpdateDataRoomEntryResult::Success(success) = &result {
            assert!(!success.inserted_records.is_empty());
            let last_inserted_record = &(success.inserted_records.last().unwrap().1);

            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_MOLECULE_DATA_ROOM_SERVICE,
                    MoleculeDataRoomMessage::moved(
                        success.system_time,
                        molecule_subject.account_id.clone(),
                        molecule_project.account_id.clone(),
                        molecule_project.ipnft_uid.clone(),
                        path_from,
                        path_to,
                        MoleculeDataRoomEntry::from_collection_entry(
                            kamu_datasets::CollectionEntry {
                                system_time: success.system_time,
                                event_time: source_event_time.unwrap_or(success.system_time),
                                path: last_inserted_record.path.clone(),
                                reference: last_inserted_record.reference.clone(),
                                extra_data: last_inserted_record.extra_data.clone(),
                            },
                        ),
                    ),
                )
                .await
                .int_err()?;
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
