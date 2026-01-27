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
use internal_error::{InternalError, ResultIntoInternal};
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

impl MoleculeAppendGlobalDataRoomActivityUseCaseImpl {
    async fn get_offset_after_append(
        &self,
        global_activities_dataset: &dyn odf::Dataset,
        new_head: &odf::Multihash,
    ) -> Result<u64, InternalError> {
        // Extract the event from the new head
        let new_odf_event = global_activities_dataset
            .as_metadata_chain()
            .get_block(new_head)
            .await
            .int_err()?
            .event;

        // We expect it to be AddData
        let odf::metadata::MetadataEvent::AddData(add_data) = new_odf_event else {
            unreachable!("We just appended data, the latest event must be AddData")
        };

        // We expect just 1 record to be appended
        let offset = if let Some(new_data) = add_data.new_data {
            assert_eq!(new_data.num_records(), 1);
            new_data.offset_interval.end
        } else {
            unreachable!("We just appended data, new_data must be Some")
        };

        Ok(offset)
    }
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
            .await?;

        // Check commit status
        match push_res {
            PushIngestResult::UpToDate => {
                unreachable!("We just created a new announcement, it cannot be up-to-date")
            }
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                new_head,
                ..
            } => {
                // We need to know offset for message
                let offset = self
                    .get_offset_after_append(
                        global_data_room_activities_writer
                            .get_write_checked_dataset()
                            .as_ref(),
                        &new_head,
                    )
                    .await?;

                // Notify external listeners
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
                        MoleculeActivityMessage::published(
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            offset,
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
