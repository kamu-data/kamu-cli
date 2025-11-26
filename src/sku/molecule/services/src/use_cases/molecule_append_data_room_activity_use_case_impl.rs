// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::auth;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAppendDataRoomActivityUseCase)]
pub struct MoleculeAppendDataRoomActivityUseCaseImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
    push_ingest_use_case: Arc<dyn kamu_core::PushIngestDataUseCase>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeAppendDataRoomActivityUseCase for MoleculeAppendDataRoomActivityUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeAppendDataRoomActivityUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        activity: MoleculeDataRoomActivityEntity,
    ) -> Result<(), MoleculeAppendDataRoomActivityError> {
        let data_room_activity_dataset = self
            .molecule_dataset_service
            .get_global_data_room_activity_dataset(
                &molecule_subject.account_name,
                auth::DatasetAction::Write,
                // TODO: try to create once as start-up job?
                true,
            )
            .await
            .map_err(|e| -> MoleculeAppendDataRoomActivityError {
                use MoleculeGetDatasetError as E;

                match e {
                    MoleculeGetDatasetError::NotFound(_) => {
                        unreachable!()
                    }
                    MoleculeGetDatasetError::Access(e) => e.into(),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        let data_record = activity.into_insert_record();

        self.push_ingest_use_case
            .execute(
                data_room_activity_dataset,
                kamu_core::DataSource::Buffer(data_record.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(file_utils::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
