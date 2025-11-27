// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ResultIntoInternal;
use kamu_accounts::LoggedAccount;
use kamu_core::auth;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewDataRoomActivitiesUseCase)]
pub struct MoleculeViewDataRoomActivitiesUseCaseImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewDataRoomActivitiesUseCase for MoleculeViewDataRoomActivitiesUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeViewDataRoomActivitiesUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError> {
        let Some(df) = self
            .molecule_dataset_service
            .get_global_data_room_activity_data_frame(
                &molecule_subject.account_name,
                auth::DatasetAction::Read,
                false,
            )
            .await?
            .1
        else {
            return Ok(MoleculeDataRoomActivityListing::default());
        };

        // Get the total count before pagination
        let total_count = df.clone().count().await.int_err()?;

        // Sort the df by offset descending
        use datafusion::logical_expr::col;

        let df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        // Apply pagination
        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;

        let list = records
            .into_iter()
            .map(MoleculeDataRoomActivityEntity::from_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(MoleculeDataRoomActivityListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
