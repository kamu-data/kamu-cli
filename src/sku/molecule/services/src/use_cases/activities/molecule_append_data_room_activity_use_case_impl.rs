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
use internal_error::ResultIntoInternal;
use kamu_core::PushIngestResult;
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeGlobalDataRoomActivitiesService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAppendGlobalDataRoomActivityUseCase)]
pub struct MoleculeAppendGlobalDataRoomActivityUseCaseImpl {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeAppendGlobalDataRoomActivityUseCase
    for MoleculeAppendGlobalDataRoomActivityUseCaseImpl
{
    #[tracing::instrument(
        level = "info",
        name = MoleculeAppendGlobalDataRoomActivityUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        source_event_time: Option<DateTime<Utc>>,
        activity_record: MoleculeDataRoomActivityPayloadRecord,
    ) -> Result<(), MoleculeAppendDataRoomActivityError> {
        // Gain write access to global activities dataset
        let global_data_room_activities_writer = self
            .global_data_room_activities_service
            .writer(&molecule_subject.account_name, true) // TODO: try to create once as start-up job?
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeAppendDataRoomActivityError>)?;

        // Append new activity record
        let new_changelog_record = MoleculeDataRoomActivityChangelogInsertionRecord {
            op: odf::metadata::OperationType::Append,
            payload: activity_record,
        };

        let push_res = global_data_room_activities_writer
            .push_ndjson_data(new_changelog_record.to_bytes(), source_event_time)
            .await
            .int_err()?;

        // Check commit status
        match push_res {
            PushIngestResult::UpToDate => {
                unreachable!("We just created a new announcement, it cannot be up-to-date")
            }
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                ..
            } => {
                // Notify external listeners
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
                        MoleculeActivityMessage::published(
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            new_changelog_record.payload,
                        ),
                    )
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
