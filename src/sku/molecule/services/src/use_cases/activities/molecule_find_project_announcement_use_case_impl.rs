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
#[dill::interface(dyn MoleculeFindProjectAnnouncementUseCase)]
pub struct MoleculeFindProjectAnnouncementUseCaseImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeFindProjectAnnouncementUseCase for MoleculeFindProjectAnnouncementUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeFindProjectAnnouncementUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        announcement_id: uuid::Uuid,
    ) -> Result<Option<MoleculeProjectAnnouncementRecord>, MoleculeFindProjectAnnouncementError>
    {
        let (_, maybe_df) = self
            .molecule_dataset_service
            .get_project_announcements_data_frame(
                &molecule_project.announcements_dataset_id,
                auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| -> MoleculeFindProjectAnnouncementError {
                use MoleculeGetDatasetError as E;
                match e {
                    E::NotFound(_) => {
                        unreachable!()
                    }
                    E::Access(e) => e.into(),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let Some(df) = maybe_df else {
            // TODO: Verify if an empty dataframe will be returned when we define
            //       schema
            return Ok(None);
        };

        use datafusion::logical_expr::{col, lit};

        let df = df
            .filter(col("announcement_id").eq(lit(announcement_id.to_string())))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);

        let record = records.into_iter().next().unwrap();
        let entity = MoleculeProjectAnnouncementRecord::from_json(record)?;

        Ok(Some(entity))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
