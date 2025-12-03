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
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{QueryService, auth};
use kamu_datasets::*;
use kamu_molecule_domain::*;
use odf::utils::data::DataFrameExt;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeProjectAnnouncementEntriesService)]
pub struct MoleculeProjectAnnouncementCollectionServiceImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
}

impl MoleculeProjectAnnouncementCollectionServiceImpl {
    async fn get_project_announcements_data_frame(
        &self,
        announcement_dataset_id: &odf::DatasetID,
    ) -> Result<Option<DataFrameExt>, MoleculeAnnouncementsCollectionReadError> {
        self.molecule_dataset_service
            .get_project_announcements_data_frame(
                announcement_dataset_id,
                auth::DatasetAction::Read,
            )
            .await
            .map(|(_, df)| df)
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => e.into(),
                RebacDatasetRefUnresolvedError::Access(e) => e.into(),
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeProjectAnnouncementEntriesService
    for MoleculeProjectAnnouncementCollectionServiceImpl
{
    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectAnnouncementCollectionServiceImpl_get_project_announcement_entries,
        skip_all,
        fields(%project_announcements_dataset_id, ?as_of, ?pagination)
    )]
    async fn get_project_announcement_entries(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeAnnouncementsCollectionReadError> {
        let readable_announcement_dataset = self
            .readable_announcement_dataset(project_announcements_dataset_id)
            .await?;

        let entries_listing = self
            .view_collection_entries
            .execute(
                ReadCheckedDataset(&readable_announcement_dataset),
                as_of,
                None,
                None,
                pagination,
            )
            .await
            .map_err(|e| match e {
                ViewCollectionEntriesError::Access(e) => e.into(),
                e @ ViewCollectionEntriesError::Internal(_) => e.int_err().into(),
            })?;

        Ok(entries_listing)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeProjectAnnouncementCollectionServiceImpl_find_project_announcement_entry_by_id,
        skip_all,
        fields(%project_announcements_dataset_id, ?as_of, %announcement_id)
    )]
    async fn find_project_announcement_entry_by_id(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        announcement_id: uuid::Uuid,
    ) -> Result<Option<MoleculeProjectAnnouncementRecord>, MoleculeAnnouncementsCollectionReadError>
    {
        let _ = project_announcements_dataset_id;
        let _ = as_of;
        let _ = announcement_id;
        todo!()
        // let readable_data_room = self
        //     .readable_announcement_dataset(project_announcements_dataset_id)
        //     .await?;
        //
        // use datafusion::logical_expr::{col, lit};
        //
        // let Some(df) = self
        //     .query_service
        //     .get_data(
        //         collection_dataset.clone(),
        //         GetDataOptions {
        //             block_hash: as_of.clone(),
        //         },
        //     )
        //     .await
        //     .int_err()?
        //     .df
        // else {
        //     return Ok(Vec::new());
        // };
        //
        // // Apply filters
        // // Note: we are still working with a changelog here in hope to narrow
        // down the // record set before projecting
        // let df = df
        //     .filter(col("ref").in_list(refs.iter().map(|r|
        // lit(r.to_string())).collect(), false))     .int_err()?;
        //
        // // Project changelog into a state
        // let df = odf::utils::data::changelog::project(
        //     df,
        //     &["path".to_string()],
        //     &odf::metadata::DatasetVocabulary::default(),
        // )
        // .int_err()?;
        //
        // let df = df.sort(vec![col("path").sort(true, false)]).int_err()?;
        //
        // let records = df.collect_json_aos().await.int_err()?;
        //
        // let nodes = records
        //     .into_iter()
        //     .map(CollectionEntry::from_json)
        //     .collect::<Result<_, _>>()?;
        //
        // Ok(maybe_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
