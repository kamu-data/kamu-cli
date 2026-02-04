// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::PushIngestResult;
use kamu_molecule_domain::*;
use messaging_outbox::*;

use crate::MoleculeGlobalDataRoomActivitiesService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<MoleculeActivityMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_MOLECULE_GLOBAL_ACTIVITY_WRITER,
    feeding_producers: &[
        MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct MoleculeAsyncGlobalActivityWriter {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeAsyncGlobalActivityWriter {
    async fn handle_write_requested_message(
        &self,
        MoleculeActivityMessageWriteRequested {
            molecule_subject_account_name,
            molecule_subject_account_id,
            source_event_time,
            activity_record,
        }: &MoleculeActivityMessageWriteRequested,
    ) -> Result<(), InternalError> {
        // Append new activity record
        let new_changelog_record = MoleculeDataRoomActivityChangelogInsertionRecord {
            op: odf::metadata::OperationType::Append,
            payload: activity_record.clone(),
        };

        // Gain write access to global activities dataset
        let global_data_room_activities_writer = self
            .global_data_room_activities_service
            .writer(molecule_subject_account_name, true) // TODO: try to create once as start-up job?
            .await
            .int_err()?;

        // TODO: several attempts
        let push_res = global_data_room_activities_writer
            .push_ndjson_data(new_changelog_record.to_bytes(), *source_event_time)
            .await
            .int_err()?;

        //  Check commit status
        match push_res {
            PushIngestResult::UpToDate => {
                unreachable!("We just ingested a new global activity, it cannot be up-to-date")
            }
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                new_head,
                ..
            } => {
                // We need to know the offset for a message
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
                            source_event_time.unwrap_or(insertion_system_time),
                            molecule_subject_account_id.clone(),
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

impl MessageConsumer for MoleculeAsyncGlobalActivityWriter {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<MoleculeActivityMessage> for MoleculeAsyncGlobalActivityWriter {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "MoleculeGlobalActivityWriter[MoleculeActivityMessage]"
    )]
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &MoleculeActivityMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received Molecule activity message");

        match message {
            MoleculeActivityMessage::Published(_) => {
                // No action required
                Ok(())
            }
            MoleculeActivityMessage::WriteRequested(message) => {
                self.handle_write_requested_message(message).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
