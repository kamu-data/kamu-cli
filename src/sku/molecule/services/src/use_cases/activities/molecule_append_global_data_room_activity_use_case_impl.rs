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
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeGlobalDataRoomActivitiesService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAppendGlobalDataRoomActivityUseCase)]
pub struct MoleculeAppendGlobalDataRoomActivityUseCaseImpl {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    account_quota_storage_checker: Arc<dyn kamu_accounts::AccountQuotaStorageChecker>,
    outbox: Arc<dyn Outbox>,
}

impl MoleculeAppendGlobalDataRoomActivityUseCaseImpl {
    async fn ensure_within_quota(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        activity_record: &MoleculeDataRoomActivityPayloadRecord,
    ) -> Result<(), kamu_accounts::QuotaError> {
        let roughly_estimated_size = activity_record.roughly_estimated_size_in_bytes() as u64;

        self.account_quota_storage_checker
            .ensure_within_quota(&molecule_subject.account_id, roughly_estimated_size)
            .await
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
        // Check dataset access before sending the message
        let _ = self
            .global_data_room_activities_service
            .writer(&molecule_subject.account_name, true) // TODO: try to create once as start-up job?
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeAppendDataRoomActivityError>)?;

        self.ensure_within_quota(molecule_subject, &activity_record)
            .await?;

        // Request asynchronous write
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_MOLECULE_ACTIVITY_SERVICE,
                MoleculeActivityMessage::write_requested(
                    molecule_subject.account_name.clone(),
                    molecule_subject.account_id.clone(),
                    source_event_time,
                    activity_record,
                ),
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
