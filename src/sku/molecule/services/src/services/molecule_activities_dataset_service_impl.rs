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
use odf::utils::data::DataFrameExt;

use super::molecule_datasets_helper::MoleculeDatasetsHelper;
use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeActivitiesDatasetService)]
pub struct MoleculeActivitiesDatasetServiceImpl {
    rebac_dataset_registry: Arc<dyn RebacDatasetRegistryFacade>,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    query_service: Arc<dyn QueryService>,
}

impl MoleculeActivitiesDatasetServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeActivitiesDatasetService for MoleculeActivitiesDatasetServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeActivitiesDatasetServiceImpl_get_global_data_room_activity_dataset,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_global_data_room_activity_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError> {
        let data_room_activity_dataset_alias =
            MoleculeDatasetSnapshots::global_data_room_activity_alias(
                molecule_account_name.clone(),
            );

        MoleculeDatasetsHelper::get_or_create_dataset(
            self.create_dataset_use_case.as_ref(),
            self.rebac_dataset_registry.as_ref(),
            &data_room_activity_dataset_alias.as_local_ref(),
            action,
            create_if_not_exist,
            || MoleculeDatasetSnapshots::global_data_room_activity(molecule_account_name.clone()),
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeActivitiesDatasetServiceImpl_get_global_data_room_activity_data_frame,
        skip_all,
        fields(molecule_account_name, ?action, create_if_not_exist)
    )]
    async fn get_global_data_room_activity_data_frame(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError> {
        let data_room_activity_dataset = self
            .get_global_data_room_activity_dataset(
                molecule_account_name,
                action,
                create_if_not_exist,
            )
            .await?;

        let maybe_df = MoleculeDatasetsHelper::try_get_data_frame(
            self.query_service.as_ref(),
            data_room_activity_dataset.clone(),
        )
        .await?;

        Ok((data_room_activity_dataset, maybe_df))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
