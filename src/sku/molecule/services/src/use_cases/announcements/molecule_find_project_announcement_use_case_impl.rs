// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_core::auth;
use kamu_molecule_domain::*;

use crate::MoleculeAnnouncementsDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeFindProjectAnnouncementUseCase)]
pub struct MoleculeFindProjectAnnouncementUseCaseImpl {
    molecule_announcements_dataset_service: Arc<dyn MoleculeAnnouncementsDatasetService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeFindProjectAnnouncementUseCase for MoleculeFindProjectAnnouncementUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeFindProjectAnnouncementUseCaseImpl_execute
        skip_all,
        fields(ipnft_uid = %molecule_project.ipnft_uid, id = %id)
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        id: uuid::Uuid,
    ) -> Result<Option<MoleculeProjectAnnouncementRecord>, MoleculeFindProjectAnnouncementError>
    {
        let (_, maybe_df) = self
            .molecule_announcements_dataset_service
            .get_project_announcements_data_frame(
                &molecule_project.announcements_dataset_id,
                auth::DatasetAction::Read,
            )
            .await
            .int_err()?;

        let Some(df) = maybe_df else {
            return Ok(None);
        };

        use datafusion::logical_expr::{col, lit};

        // TODO: add col const from snapshot?
        let df = df
            .filter(col("announcement_id").eq(lit(id.as_hyphenated().to_string())))
            .int_err()?;
        let records = df.collect_json_aos().await.int_err()?;

        let announcement_record = records
            .into_iter()
            .next()
            .map(MoleculeProjectAnnouncementRecord::from_json)
            .transpose()?;

        Ok(announcement_record)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
