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
use kamu_molecule_domain::*;

use crate::MoleculeDatasetAccessorFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeFindProjectAnnouncementUseCase)]
pub struct MoleculeFindProjectAnnouncementUseCaseImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
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
        let maybe_df = self
            .accessor_factory
            .read_accessor(&molecule_project.announcements_dataset_id.as_local_ref())
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectAnnouncementError>)?
            .try_get_raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectAnnouncementError>)?;

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
