// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::{QueryService, auth};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, ResolvedDataset};
use kamu_molecule_domain::*;
use odf::utils::data::DataFrameExt;

use super::molecule_datasets_helper::MoleculeDatasetsHelper;
use crate::MoleculeAnnouncementsDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeAnnouncementsDatasetService)]
pub struct MoleculeAnnouncementsDatasetServiceImpl {
    rebac_dataset_registry: Arc<dyn RebacDatasetRegistryFacade>,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeAnnouncementsDatasetService for MoleculeAnnouncementsDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_get_global_announcements_dataset,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_global_announcements_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        let announcements_dataset_alias =
            MoleculeDatasetSnapshots::global_announcements_alias(molecule_account_name.clone());

        MoleculeDatasetsHelper::get_or_create_dataset(
            self.create_dataset_use_case.as_ref(),
            self.rebac_dataset_registry.as_ref(),
            &announcements_dataset_alias.as_local_ref(),
            action,
            create_if_not_exist,
            || MoleculeDatasetSnapshots::global_announcements(molecule_account_name.clone()),
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_get_global_announcements_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_global_announcements_data_frame(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
        let announcements_dataset = self
            .get_global_announcements_dataset(molecule_account_name, action, create_if_not_exist)
            .await?;

        let maybe_df = MoleculeDatasetsHelper::try_get_data_frame(
            self.query_service.as_ref(),
            announcements_dataset.clone(),
        )
        .await?;

        Ok((announcements_dataset, maybe_df))
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_get_project_announcements_dataset,
        skip_all,
        fields(molecule_account_name, ?action, project_announcements_dataset_id)
    )]
    async fn get_project_announcements_dataset(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        MoleculeDatasetsHelper::get_or_create_dataset(
            self.create_dataset_use_case.as_ref(),
            self.rebac_dataset_registry.as_ref(),
            &project_announcements_dataset_id.as_local_ref(),
            action,
            false, // already created
            || unreachable!(),
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeAnnouncementsDatasetServiceImpl_get_project_announcements_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, project_announcements_dataset_id)
    )]
    async fn get_project_announcements_data_frame(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        action: auth::DatasetAction,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
        let project_announcements_dataset = self
            .get_project_announcements_dataset(project_announcements_dataset_id, action)
            .await?;

        let maybe_df = MoleculeDatasetsHelper::try_get_data_frame(
            self.query_service.as_ref(),
            project_announcements_dataset.clone(),
        )
        .await?;

        Ok((project_announcements_dataset, maybe_df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
