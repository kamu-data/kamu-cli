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

use crate::MoleculeActivitiesDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAppendGlobalDataRoomActivityUseCase)]
pub struct MoleculeAppendGlobalDataRoomActivityUseCaseImpl {
    molecule_activities_dataset_service: Arc<dyn MoleculeActivitiesDatasetService>,
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
        activity: MoleculeDataRoomActivityEntity,
    ) -> Result<(), MoleculeAppendDataRoomActivityError> {
        let global_activities_accessor = self
            .molecule_activities_dataset_service
            .request_write_of_global_activity_dataset(&molecule_subject.account_name, true) // TODO: try to create once as start-up job?
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeAppendDataRoomActivityError>)?;

        let data_record = activity.into_insert_record();

        global_activities_accessor
            .push_ndjson_data(data_record.to_bytes(), source_event_time)
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
