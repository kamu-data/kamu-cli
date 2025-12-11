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
use kamu_molecule_domain::*;

use crate::MoleculeDatasetAccessorFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectAnnouncementsUseCase)]
pub struct MoleculeViewProjectAnnouncementsUseCaseImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewProjectAnnouncementsUseCase for MoleculeViewProjectAnnouncementsUseCaseImpl {
    #[tracing::instrument(
        name = MoleculeViewProjectAnnouncementsUseCaseImpl_execute,
        level = "debug",
        skip_all
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeViewProjectAnnouncementsError> {
        let maybe_df = self
            .accessor_factory
            .read_accessor(&molecule_project.announcements_dataset_id.as_local_ref())
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectAnnouncementsError>)?
            .try_get_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectAnnouncementsError>)?;

        let Some(df) = maybe_df else {
            return Ok(MoleculeProjectAnnouncementListing::default());
        };

        use datafusion::logical_expr::col;

        // Sort the df by offset descending
        // TODO: add col const from snapshot?
        let df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        // Apply pagination
        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        let maybe_filter = filters.and_then(|f| {
            kamu_molecule_domain::molecule_extra_data_fields_filter(
                f.by_tags,
                f.by_categories,
                f.by_access_levels,
            )
        });

        let df = if let Some(filters) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filters)
                .int_err()?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let announcements = records
            .into_iter()
            .map(MoleculeProjectAnnouncementRecord::from_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(MoleculeProjectAnnouncementListing {
            total_count,
            list: announcements,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
